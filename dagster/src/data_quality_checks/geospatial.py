import networkx as nx
import numpy as np
import pandas as pd
from gigaspatial.config import config as gigaspatial_config
from gigaspatial.core.io import ADLSDataStore
from gigaspatial.generators.poi import PoiViewGenerator
from gigaspatial.processing.algorithms import build_distance_graph
from networkx.algorithms.clique import find_cliques as maximal_cliques
from pyspark import sql
from pyspark.sql import functions as f
from pyspark.sql.types import LongType, StringType, StructField, StructType

from dagster import OpExecutionContext
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger

UNINHABITED_DISTANCE_THRESHOLD_M = 150

BUILT_SURFACE_THRESHOLD_M2 = 10.0

BUILT_SURFACE_BUFFER_M = 150

PROXIMITY_DUPLICATE_THRESHOLD_M = 50

# GHSL SMOD classes 21-30 (suburban through urban centre); everything below is rural.
# Ref: https://ghsl.jrc.ec.europa.eu/ghs_smod2023.php
SMOD_URBAN_CLASSES = {21, 22, 23, 30}

CONTEXT_RADII_M = [1000, 2000, 3000, 5000, 10000]


def _get_data_store() -> ADLSDataStore:
    """Return a giga-spatial ADLSDataStore scoped to giga-spatial usage."""
    if settings.GIGASPATIAL_ROOT_DATA_DIR:
        gigaspatial_config.set_path("root", settings.GIGASPATIAL_ROOT_DATA_DIR)
    return ADLSDataStore(
        container=settings.GIGASPATIAL_ADLS_CONTAINER,
        account_url=f"https://{settings.AZURE_BLOB_SAS_HOST}/",
        sas_token=settings.GIGASPATIAL_ADLS_SAS_TOKEN,
    )


def _spark_to_pandas_coords(df: sql.DataFrame) -> pd.DataFrame:
    """Extract school_id_giga, latitude, longitude (and the out-of-country flag when
    present) from Spark DF to Pandas."""
    columns = ["school_id_giga", "latitude", "longitude"]
    if "dq_is_not_within_country" in df.columns:
        columns.append("dq_is_not_within_country")
    pdf = df.select(*columns).toPandas()
    pdf["latitude"] = pd.to_numeric(pdf["latitude"], errors="coerce")
    pdf["longitude"] = pd.to_numeric(pdf["longitude"], errors="coerce")
    return pdf


def _drop_out_of_country(pdf: pd.DataFrame, logger, check_name: str) -> pd.DataFrame:
    """Drop rows flagged outside the country boundary (``dq_is_not_within_country == 1``)"""

    if "dq_is_not_within_country" not in pdf.columns:
        return pdf
    outside = pdf["dq_is_not_within_country"] == 1
    if outside.any():
        logger.info(
            f"{check_name}: dropping {int(outside.sum())} out-of-country rows "
            f"(dq_is_not_within_country == 1)"
        )
    return pdf[~outside].copy()


def _get_valid_coords_mask(pdf: pd.DataFrame) -> pd.Series:
    """Return a boolean mask for rows with valid (non-null, non-NaN) coordinates."""
    return (
        pdf["latitude"].notna()
        & pdf["longitude"].notna()
        & pdf["latitude"].between(-90, 90)
        & pdf["longitude"].between(-180, 180)
    )


def _log_coordinate_summary(
    logger,
    check_name: str,
    total_rows: int,
    valid_rows: int,
) -> None:
    logger.info(
        f"{check_name}: coordinates total={total_rows}, valid={valid_rows}, invalid={total_rows - valid_rows}"
    )


def _log_non_null_counts(
    logger, check_name: str, pdf: pd.DataFrame, columns: list[str]
):
    counts = {col: int(pdf[col].notna().sum()) for col in columns if col in pdf.columns}
    logger.info(f"{check_name}: non-null output counts={counts}")


def _log_coord_finiteness(
    logger, check_name: str, pdf: pd.DataFrame, dump_offending: bool = False
) -> None:
    """Log coordinate finiteness diagnostics to pin down 'data must be finite' errors.

    ``pdf`` is expected to already be coordinate-masked, so any non-finite/pole value
    or duplicate/null id logged here is what reaches the distance-graph builder.
    """
    lat = pd.to_numeric(pdf["latitude"], errors="coerce")
    lon = pd.to_numeric(pdf["longitude"], errors="coerce")
    lat_nonfinite = ~np.isfinite(lat)
    lon_nonfinite = ~np.isfinite(lon)
    at_pole = lat.abs() >= 90
    logger.info(
        f"{check_name}: coord finiteness rows={len(pdf)} "
        f"lat[min={lat.min()}, max={lat.max()}] lon[min={lon.min()}, max={lon.max()}] "
        f"nonfinite_lat={int(lat_nonfinite.sum())} nonfinite_lon={int(lon_nonfinite.sum())} "
        f"at_pole={int(at_pole.sum())} "
        f"null_id={int(pdf['school_id_giga'].isna().sum())} "
        f"dup_id={int(pdf['school_id_giga'].duplicated().sum())}"
    )
    if dump_offending:
        offending = pdf[lat_nonfinite | lon_nonfinite | at_pole]
        logger.error(
            f"{check_name}: offending coordinate rows ({len(offending)}): "
            f"{offending[['school_id_giga', 'latitude', 'longitude']].head(20).to_dict('records')}"
        )


def _prepare_poi_points(
    pdf_valid: pd.DataFrame, logger, check_name: str
) -> pd.DataFrame:
    """Drop null and duplicate ``school_id_giga`` rows so it can be used as a unique
    ``poi_id_column``.

    Deduping is safe: ``school_id_giga`` is a deterministic hash of the coordinates
    (among other fields), so rows sharing an id share coordinates and therefore the
    same mapped result. Results are joined back onto the full DataFrame by
    ``school_id_giga``, which fans the single computed value out to every row that
    shares the id; rows with a null id can never be mapped and stay null.
    """
    before = len(pdf_valid)
    prepared = pdf_valid[pdf_valid["school_id_giga"].notna()].drop_duplicates(
        subset="school_id_giga", keep="first"
    )
    null_ids = before - int(pdf_valid["school_id_giga"].notna().sum())
    logger.info(
        f"{check_name}: poi points {before} -> {len(prepared)} "
        f"(dropped {null_ids} null-id, {before - null_ids - len(prepared)} duplicate-id rows)"
    )
    return prepared


def _join_pandas_result_to_spark(
    df: sql.DataFrame,
    result_pdf: pd.DataFrame,
    result_columns: list[str],
    schema: StructType = None,
) -> sql.DataFrame:
    """Join a Pandas result DataFrame back to the original Spark DataFrame
    using school_id_giga as the join key.
    """
    spark = df.sparkSession
    result_keyed = result_pdf[["school_id_giga"] + result_columns].drop_duplicates(
        subset="school_id_giga", keep="first"
    )
    result_sdf = spark.createDataFrame(result_keyed, schema=schema)
    return df.join(result_sdf, on="school_id_giga", how="left")


def _null_guard_int(df: sql.DataFrame, col_name: str) -> sql.DataFrame:
    """Set column to NULL for rows with invalid coordinates, then cast to int."""
    df = df.withColumn(
        col_name,
        f.when(
            f.col("latitude").isNull()
            | f.isnan(f.col("latitude"))
            | f.col("longitude").isNull()
            | f.isnan(f.col("longitude")),
            f.lit(None).cast("int"),
        ).otherwise(f.col(col_name)),
    )
    return df.withColumn(col_name, f.col(col_name).cast("int"))


def _null_guard_long(df: sql.DataFrame, col_name: str) -> sql.DataFrame:
    """Set column to NULL for rows with invalid coordinates, then cast to long."""
    df = df.withColumn(
        col_name,
        f.when(
            f.col("latitude").isNull()
            | f.isnan(f.col("latitude"))
            | f.col("longitude").isNull()
            | f.isnan(f.col("longitude")),
            f.lit(None).cast("long"),
        ).otherwise(f.col(col_name)),
    )
    return df.withColumn(col_name, f.col(col_name).cast("long"))


def _null_guard_string(df: sql.DataFrame, col_name: str) -> sql.DataFrame:
    """Set column to NULL for rows with invalid coordinates (string type)."""
    return df.withColumn(
        col_name,
        f.when(
            f.col("latitude").isNull()
            | f.isnan(f.col("latitude"))
            | f.col("longitude").isNull()
            | f.isnan(f.col("longitude")),
            f.lit(None).cast("string"),
        ).otherwise(f.col(col_name)),
    )


def _is_complete_graph(G: nx.Graph) -> bool:
    """Check if graph is complete (all nodes connected to each other)."""
    n = len(G.nodes)
    m = len(G.edges)
    required_edges = n * (n - 1) // 2
    return m == required_edges


def _partition_graph_by_max_cliques(G: nx.Graph) -> list:
    """Partition graph into maximal cliques for duplicate grouping."""

    def total_clique_weight(G, clique):
        return sum(
            G[u][v].get("weight", 1)
            for u in clique
            for v in clique
            if u < v and G.has_edge(u, v)
        )

    G2 = G.copy()
    cliques = []
    while len(G2.nodes) > 0:
        all_cliques = list(maximal_cliques(G2))
        max_size = max(len(c) for c in all_cliques)
        largest_cliques = [c for c in all_cliques if len(c) == max_size]
        chosen_clique = min(largest_cliques, key=lambda c: total_clique_weight(G2, c))
        cliques.append(chosen_clique)
        G2.remove_nodes_from(chosen_clique)
    return cliques


class POIContextEnricher(PoiViewGenerator):
    """Enriches POIs with contextual geospatial features from giga-spatial.

    Adds:
        - uninhabited, suspect
        - duplicate_{threshold}_flag / _group_id / _count
        - settlement_type
        - pop_within_{n}km
        - poi_count_{n}km
    """

    def __init__(
        self,
        points: pd.DataFrame,
        country_code_iso3: str,
        poi_id_column: str = "poi_id",
        data_store: ADLSDataStore = None,
        logger=None,
        radii_meters: list[int] = None,
        duplicate_threshold_m: int = PROXIMITY_DUPLICATE_THRESHOLD_M,
    ):
        super().__init__(
            points=points,
            poi_id_column=poi_id_column,
            data_store=data_store,
        )
        self.country_code_iso3 = country_code_iso3
        self.radii_meters = radii_meters or CONTEXT_RADII_M
        self.duplicate_threshold_m = duplicate_threshold_m
        self.log = logger or self.logger

    @property
    def uninhabited_columns(self) -> list[str]:
        return ["uninhabited", "suspect"]

    @property
    def duplicate_columns(self) -> list[str]:
        t = self.duplicate_threshold_m
        return [
            f"duplicate_{t}_flag",
            f"duplicate_{t}_group_id",
            f"duplicate_{t}_count",
        ]

    @property
    def settlement_columns(self) -> list[str]:
        return ["settlement_type"]

    @property
    def population_columns(self) -> list[str]:
        return [f"pop_within_{r // 1000}km" for r in self.radii_meters]

    @property
    def density_columns(self) -> list[str]:
        return [f"poi_count_{r // 1000}km" for r in self.radii_meters]

    def add_uninhabited_flags(self) -> None:
        """Flag POIs with no built environment nearby.

        ``uninhabited`` requires every available signal to agree, ``suspect`` requires
        any. A POI with no signal at all stays NULL rather than False: in a DQ context
        "not evaluated" must stay distinguishable from "evaluated, clean".
        """
        search_radius = UNINHABITED_DISTANCE_THRESHOLD_M
        self.log.info(f"Mapping nearest buildings within {search_radius}m...")
        self.find_nearest_buildings(
            country=self.country_code_iso3,
            search_radius=search_radius,
        )
        self.log.info("Mapping built surface area (GHSL GHS_BUILT_S)...")
        self.map_built_s(map_radius_meters=BUILT_SURFACE_BUFFER_M)

        view = self.view.copy()
        self.log.info(f"Uninhabited flags: giga-spatial columns={list(view.columns)}")

        # Each signal contributes a (condition, availability) pair so that a missing
        # value is excluded from the all/any reduction instead of counting as False.
        # nearest_building_distance_m is deliberately unused: wherever it is non-null,
        # "distance > search_radius" is the exact complement of building_within, so it
        # would add a duplicate vote and skew the all/any reduction.
        signals = []
        within_column = f"building_within_{search_radius}m"
        if within_column in view.columns:
            available = view[within_column].notna()
            signals.append((~view[within_column].fillna(False).astype(bool), available))
        if "built_surface_m2" in view.columns:
            available = view["built_surface_m2"].notna()
            signals.append(
                (view["built_surface_m2"] < BUILT_SURFACE_THRESHOLD_M2, available)
            )

        if not signals:
            self.log.warning("No built environment signals available")
            for column in self.uninhabited_columns:
                view[column] = pd.NA
            self._update_view(view[["poi_id"] + self.uninhabited_columns])
            return

        available_count = sum(avail.astype(int) for _, avail in signals)
        agreeing_count = sum((cond & avail).astype(int) for cond, avail in signals)
        evaluated = available_count > 0

        view["uninhabited"] = (
            pd.Series(
                np.where(agreeing_count == available_count, 1, 0), index=view.index
            )
            .where(evaluated)
            .astype("Int64")
        )
        view["suspect"] = (
            pd.Series(np.where(agreeing_count > 0, 1, 0), index=view.index)
            .where(evaluated)
            .astype("Int64")
        )

        self._update_view(view[["poi_id"] + self.uninhabited_columns])

    def add_duplicate_groups(self) -> None:
        """Cluster POIs that sit within ``duplicate_threshold_m`` of each other.

        Connected components are partitioned into maximal cliques so that a chain of
        near-neighbours is not collapsed into one oversized group.
        """
        threshold = self.duplicate_threshold_m
        self.log.info(f"Detecting duplicate locations within {threshold}m...")

        flag_column, group_column, count_column = self.duplicate_columns

        gdf = self.to_geodataframe().set_index("poi_id")
        # exclude_same_index is set rather than left to build_distance_graph's
        # left_df.equals(right_df) auto-detection: the view carries nullable dtypes by
        # this point, and a False there would give every node a self-loop, which
        # nx.degree counts as 2.
        G = build_distance_graph(gdf, gdf, threshold, exclude_same_index=True)
        self.log.info(
            f"Duplicate groups: graph nodes={G.number_of_nodes()}, edges={G.number_of_edges()}"
        )

        group_id = 0
        duplicate_map = {}
        for component in nx.connected_components(G):
            cc = G.subgraph(component).copy()
            if len(cc) <= 1:
                continue
            if _is_complete_graph(cc):
                for node in cc.nodes():
                    duplicate_map[node] = group_id
                group_id += 1
            else:
                for clique in _partition_graph_by_max_cliques(cc):
                    if len(clique) > 1:
                        for node in clique:
                            duplicate_map[node] = group_id
                        group_id += 1

        degree_dict = dict(G.degree())

        view = self.view.copy()
        poi_ids = view["poi_id"]
        in_graph = poi_ids.isin(degree_dict)

        view[flag_column] = poi_ids.isin(duplicate_map).astype("Int64").where(in_graph)
        view[group_column] = poi_ids.map(duplicate_map).astype("Int64")
        view[count_column] = poi_ids.map(degree_dict).astype("Int64")

        self.log.info(
            f"Duplicate groups: {len(duplicate_map)} POIs in {group_id} groups"
        )
        self._update_view(view[["poi_id"] + self.duplicate_columns])

    def add_settlement_classification(self) -> None:
        """Classify POIs as Urban/Rural from the GHSL Settlement Model."""
        self.log.info("Mapping GHSL Settlement Model (SMOD)...")
        self.map_smod(output_column="smod_class")

        view = self.view.copy()
        if "smod_class" not in view.columns:
            self.log.warning("No SMOD data available")
            view["settlement_type"] = pd.NA
            self._update_view(view[["poi_id"] + self.settlement_columns])
            return

        smod = view["smod_class"].astype("Int64")
        view["settlement_type"] = pd.Series(
            np.where(smod.isin(SMOD_URBAN_CLASSES), "Urban", "Rural"), index=view.index
        ).where(smod.notna())

        self._update_view(view[["poi_id"] + self.settlement_columns])

    def add_population_context(self) -> None:
        """Add WorldPop population counts at each radius.

        Only ``resolution`` is passed explicitly; ``map_wp_pop`` fans its **kwargs into
        map_zonal_stats/TifProcessor as well as the handler, so the remaining WorldPop
        selectors are left on their handler defaults (GR2 / pop / 2025 / constrained).
        """
        for radius in self.radii_meters:
            column = f"pop_within_{radius // 1000}km"
            self.log.info(f"Computing {column} (radius={radius}m)...")
            # Isolated per radius so one unavailable raster nulls only its own column.
            try:
                self.map_wp_pop(
                    country=self.country_code_iso3,
                    map_radius_meters=radius,
                    resolution=1000,
                    predicate="centroid_within",
                    output_column=column,
                )
            except Exception as e:
                self.log.error(f"Population context failed for {column}: {e}")

    def add_poi_density(self) -> None:
        """Count neighbouring POIs within each radius."""
        for radius in self.radii_meters:
            column = f"poi_count_{radius // 1000}km"
            self.log.info(f"Computing {column} (radius={radius}m)...")

            # Isolated per radius so one failure nulls only its own column.
            try:
                # poi_id is dropped because map_zonal_stats uses it as the zone id column.
                result = self.map_zonal_stats(
                    data=self.to_geodataframe().drop(columns="poi_id"),
                    stat="count",
                    map_radius_meters=radius,
                    output_column=column,
                )
                # Each buffer contains its own POI.
                result[column] = result[column] - 1

                # map_zonal_stats returns a frame rather than updating the view itself.
                self._update_view(result)
            except Exception as e:
                self.log.error(f"POI density failed for {column}: {e}")


GEOSPATIAL_COLUMN_MAPPING = {
    "uninhabited": "dq_is_in_uninhabited_area",
    "suspect": "dq_is_suspect_location",
    f"duplicate_{PROXIMITY_DUPLICATE_THRESHOLD_M}_flag": "dq_duplicate_group_flag_50m",
    f"duplicate_{PROXIMITY_DUPLICATE_THRESHOLD_M}_group_id": "dq_duplicate_group_id_50m",
    f"duplicate_{PROXIMITY_DUPLICATE_THRESHOLD_M}_count": "dq_duplicate_group_count_50m",
    "settlement_type": "rurban_detected",
    **{
        f"pop_within_{r // 1000}km": f"pop_within_{r // 1000}km"
        for r in CONTEXT_RADII_M
    },
    **{
        f"poi_count_{r // 1000}km": f"schools_within_{r // 1000}km"
        for r in CONTEXT_RADII_M
    },
}

GEOSPATIAL_INT_COLUMNS = [
    "dq_is_in_uninhabited_area",
    "dq_is_suspect_location",
    "dq_duplicate_group_flag_50m",
    "dq_duplicate_group_id_50m",
    "dq_duplicate_group_count_50m",
    *[f"schools_within_{r // 1000}km" for r in CONTEXT_RADII_M],
]

GEOSPATIAL_LONG_COLUMNS = [f"pop_within_{r // 1000}km" for r in CONTEXT_RADII_M]

GEOSPATIAL_STRING_COLUMNS = ["rurban_detected"]

GEOSPATIAL_COLUMNS = (
    GEOSPATIAL_INT_COLUMNS + GEOSPATIAL_LONG_COLUMNS + GEOSPATIAL_STRING_COLUMNS
)


def _geospatial_result_schema() -> StructType:
    """Explicit schema so all-null result columns still get a usable Spark type.

    Driven off GEOSPATIAL_COLUMNS in order, so reordering that list cannot silently
    shift values between columns of the same type.
    """
    types = {c: StringType() for c in GEOSPATIAL_STRING_COLUMNS}
    types.update(
        {c: LongType() for c in GEOSPATIAL_INT_COLUMNS + GEOSPATIAL_LONG_COLUMNS}
    )
    return StructType(
        [StructField("school_id_giga", StringType(), True)]
        + [StructField(c, types[c], True) for c in GEOSPATIAL_COLUMNS]
    )


def _null_geospatial_columns(df: sql.DataFrame) -> sql.DataFrame:
    for column in GEOSPATIAL_INT_COLUMNS:
        df = df.withColumn(column, f.lit(None).cast("int"))
    for column in GEOSPATIAL_LONG_COLUMNS:
        df = df.withColumn(column, f.lit(None).cast("long"))
    for column in GEOSPATIAL_STRING_COLUMNS:
        df = df.withColumn(column, f.lit(None).cast("string"))
    return df


def _to_spark_safe(pdf: pd.DataFrame) -> pd.DataFrame:
    """Convert nullable pandas dtypes to object columns Spark can infer from."""
    for column in GEOSPATIAL_INT_COLUMNS + GEOSPATIAL_LONG_COLUMNS:
        pdf[column] = [None if pd.isna(v) else int(v) for v in pdf[column]]
    for column in GEOSPATIAL_STRING_COLUMNS:
        pdf[column] = [None if pd.isna(v) else str(v) for v in pdf[column]]
    return pdf


def run_geospatial_checks(
    df: sql.DataFrame,
    country_code_iso3: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Orchestrate all geospatial checks against a single shared POI view.

    Builds one POIContextEnricher, runs each enrichment step in isolation so that a
    giga-spatial failure nulls only its own columns, then joins the whole result back
    to Spark once.
    """
    logger = get_context_with_fallback_logger(context)
    logger.info(f"Starting geospatial checks for country={country_code_iso3}...")
    logger.info(f"GIGASPATIAL_ROOT_DATA_DIR={settings.GIGASPATIAL_ROOT_DATA_DIR}")

    initial_count = df.count()
    logger.info(f"Input rows: {initial_count}")

    pdf = _spark_to_pandas_coords(df)
    valid_mask = _get_valid_coords_mask(pdf)
    pdf_valid = pdf[valid_mask].copy()
    _log_coordinate_summary(logger, "Geospatial checks", len(pdf), len(pdf_valid))

    pdf_valid = _drop_out_of_country(pdf_valid, logger, "Geospatial checks")

    poi_points = _prepare_poi_points(pdf_valid, logger, "Geospatial checks")
    if poi_points.empty:
        logger.warning("No valid coordinates found for geospatial checks")
        return _null_geospatial_columns(df)

    _log_coord_finiteness(logger, "Geospatial checks", poi_points)

    try:
        enricher = POIContextEnricher(
            points=poi_points,
            country_code_iso3=country_code_iso3,
            poi_id_column="school_id_giga",
            data_store=_get_data_store(),
            logger=logger,
        )
    except Exception as e:
        logger.error(f"Could not build POI view: {e}")
        return _null_geospatial_columns(df)

    steps = [
        ("Uninhabited area check", enricher.add_uninhabited_flags),
        ("Proximity duplicate check", enricher.add_duplicate_groups),
        ("SMOD classification", enricher.add_settlement_classification),
        ("Population context check", enricher.add_population_context),
        ("Surrounding schools check", enricher.add_poi_density),
    ]
    for name, step in steps:
        try:
            step()
        except Exception as e:
            logger.error(f"{name} failed: {e}")

    view = enricher.view
    result_pdf = pd.DataFrame({"school_id_giga": view["poi_id"]})
    for source, target in GEOSPATIAL_COLUMN_MAPPING.items():
        result_pdf[target] = view[source] if source in view.columns else pd.NA

    _log_non_null_counts(logger, "Geospatial checks", result_pdf, GEOSPATIAL_COLUMNS)

    existing = [c for c in GEOSPATIAL_COLUMNS if c in df.columns]
    if existing:
        logger.info(f"Dropping pre-existing geospatial columns: {existing}")
        df = df.drop(*existing)

    df = _join_pandas_result_to_spark(
        df,
        _to_spark_safe(result_pdf),
        GEOSPATIAL_COLUMNS,
        schema=_geospatial_result_schema(),
    )

    for column in GEOSPATIAL_INT_COLUMNS:
        df = _null_guard_int(df, column)
    for column in GEOSPATIAL_LONG_COLUMNS:
        df = _null_guard_long(df, column)
    for column in GEOSPATIAL_STRING_COLUMNS:
        df = _null_guard_string(df, column)

    final_count = df.count()
    logger.info(f"Geospatial checks complete. Rows: {initial_count} -> {final_count}")
    return df
