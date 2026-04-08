import geopandas as gpd
import networkx as nx
import pandas as pd
from gigaspatial.core.io import LocalDataStore
from gigaspatial.generators.poi import PoiViewGenerator
from gigaspatial.processing.algorithms import build_distance_graph
from networkx.algorithms.clique import find_cliques as maximal_cliques
from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger

# Uninhabited area threshold: if the nearest building is farther than this distance (in meters),
# the school is considered to be in an uninhabited area.
UNINHABITED_DISTANCE_THRESHOLD_M = 150

# Built surface threshold: if the total built surface area (m²) within the buffer radius
# is below this value, the location has insufficient built infrastructure.
BUILT_SURFACE_THRESHOLD_M2 = 10.0

# Buffer radius for built surface area check (in meters)
BUILT_SURFACE_BUFFER_M = 150

# Proximity duplicate threshold: schools within this distance (in meters) are considered
# potential duplicates.
PROXIMITY_DUPLICATE_THRESHOLD_M = 50

# SMOD class labels mapping
# Ref: https://ghsl.jrc.ec.europa.eu/ghs_smod2023.php
SMOD_CLASS_LABELS = {
    30: "Urban Centre",
    23: "Dense Urban Cluster",
    22: "Semi-Dense Urban Cluster",
    21: "Suburban or Peri-Urban",
    13: "Rural Cluster",
    12: "Low Density Rural",
    11: "Very Low Density Rural",
    10: "Water",
}

SMOD_URBAN_CLASSES = {20, 21, 22, 23, 30}

# Population radii to compute (in meters)
POPULATION_RADII_M = {
    "pop_within_1km": 1000,
    "pop_within_2km": 2000,
    "pop_within_3km": 3000,
    "pop_within_5km": 5000,
    "pop_within_10km": 10000,
}

SURROUNDING_SCHOOLS_RADII_M = [1000, 2000, 3000, 5000, 10000]


def _get_data_store():
    """Return a giga-spatial LocalDataStore"""
    if settings.GEOSPATIAL_DATA_DIR:
        return LocalDataStore(settings.GEOSPATIAL_DATA_DIR)
    return LocalDataStore()


def _spark_to_pandas_coords(df: sql.DataFrame) -> pd.DataFrame:
    """Extract school_id_giga, latitude, longitude from Spark DF to Pandas,
    filtering out rows with NULL/NaN coordinates.
    """
    pdf = df.select("school_id_giga", "latitude", "longitude").toPandas()
    pdf["latitude"] = pd.to_numeric(pdf["latitude"], errors="coerce")
    pdf["longitude"] = pd.to_numeric(pdf["longitude"], errors="coerce")
    return pdf


def _get_valid_coords_mask(pdf: pd.DataFrame) -> pd.Series:
    """Return a boolean mask for rows with valid (non-null, non-NaN) coordinates."""
    return (
        pdf["latitude"].notna()
        & pdf["longitude"].notna()
        & pdf["latitude"].between(-90, 90)
        & pdf["longitude"].between(-180, 180)
    )


def _join_pandas_result_to_spark(
    df: sql.DataFrame,
    result_pdf: pd.DataFrame,
    result_columns: list[str],
) -> sql.DataFrame:
    """Join a Pandas result DataFrame back to the original Spark DataFrame
    using school_id_giga as the join key.
    """
    spark = df.sparkSession
    result_sdf = spark.createDataFrame(result_pdf[["school_id_giga"] + result_columns])
    df = df.join(result_sdf, on="school_id_giga", how="left")
    return df


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


def uninhabited_area_check(  # noqa
    df: sql.DataFrame,
    country_code_iso3: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Check if schools are in uninhabited areas using multiple signals
        1. Nearest Google building distance > threshold
        2. Nearest MS building distance > threshold
        3. Built surface area (GHSL GHS_BUILT_S) within buffer < threshold

    Adds columns:
        - dq_is_in_uninhabited_area: 1 = ALL signals indicate uninhabited
        - dq_is_suspect_location: 1 = ANY signal indicates uninhabited
    """
    logger = get_context_with_fallback_logger(context)
    logger.info("Running uninhabited area check...")

    null_cols = [
        "dq_is_in_uninhabited_area",
        "dq_is_suspect_location",
    ]

    try:
        pdf = _spark_to_pandas_coords(df)
        valid_mask = _get_valid_coords_mask(pdf)
        pdf_valid = pdf[valid_mask].copy()

        if pdf_valid.empty:
            logger.warning("No valid coordinates found for uninhabited area check")
            for c in null_cols:
                df = df.withColumn(c, f.lit(None).cast("int"))
            return df

        data_store = _get_data_store()
        view = PoiViewGenerator(
            points=pdf_valid,
            poi_id_column="school_id_giga",
            data_store=data_store,
        )

        logger.info("Mapping Google Open Buildings...")
        view.map_google_buildings()
        logger.info("Mapping Microsoft Open Buildings...")
        view.map_ms_buildings()
        logger.info("Mapping built surface area (GHSL GHS_BUILT_S)...")
        view.map_built_s(map_radius_meters=BUILT_SURFACE_BUFFER_M)

        view_df = view.view
        view_indexed = view_df.set_index("poi_id")

        result_pdf = pdf.copy()
        for c in null_cols:
            result_pdf[c] = None

        for idx in result_pdf.index:
            sid = result_pdf.at[idx, "school_id_giga"]
            if sid not in view_indexed.index:
                continue

            row = view_indexed.loc[sid]
            conditions = []

            if "nearest_google_building_distance" in view_indexed.columns and pd.notna(
                row.get("nearest_google_building_distance")
            ):
                conditions.append(
                    row["nearest_google_building_distance"]
                    > UNINHABITED_DISTANCE_THRESHOLD_M
                )
            if "nearest_ms_building_distance" in view_indexed.columns and pd.notna(
                row.get("nearest_ms_building_distance")
            ):
                conditions.append(
                    row["nearest_ms_building_distance"]
                    > UNINHABITED_DISTANCE_THRESHOLD_M
                )
            if "built_surface_m2" in view_indexed.columns and pd.notna(
                row.get("built_surface_m2")
            ):
                conditions.append(row["built_surface_m2"] < BUILT_SURFACE_THRESHOLD_M2)

            if conditions:
                result_pdf.at[idx, "dq_is_in_uninhabited_area"] = (
                    1 if all(conditions) else 0
                )
                result_pdf.at[idx, "dq_is_suspect_location"] = (
                    1 if any(conditions) else 0
                )

        df = _join_pandas_result_to_spark(df, result_pdf, null_cols)

    except Exception as e:
        logger.error(f"Uninhabited area check failed: {e}")
        for c in null_cols:
            df = df.withColumn(c, f.lit(None).cast("int"))

    for c in null_cols:
        df = _null_guard_int(df, c)

    return df


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


def proximity_duplicate_check(  # noqa
    df: sql.DataFrame,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Check if schools have potential duplicates within a proximity threshold

    Adds columns:
        - dq_duplicate_group_flag_50m: 1 = has nearby duplicate, 0 = no nearby duplicate
        - dq_duplicate_group_id_50m: integer cluster ID (schools in the same cluster share an ID)
        - dq_duplicate_group_count_50m: number of direct neighbors within threshold
    """
    logger = get_context_with_fallback_logger(context)
    logger.info(
        "Running proximity duplicate check (50m, build_distance_graph + clique partitioning)..."
    )

    dup_cols = [
        "dq_duplicate_group_flag_50m",
        "dq_duplicate_group_id_50m",
        "dq_duplicate_group_count_50m",
    ]

    pdf = _spark_to_pandas_coords(df)
    valid_mask = _get_valid_coords_mask(pdf)
    pdf_valid = pdf[valid_mask].copy()

    if pdf_valid.empty or len(pdf_valid) < 2:
        logger.warning("Not enough valid coordinates for proximity duplicate check")
        for c in dup_cols:
            df = df.withColumn(c, f.lit(None).cast("int"))
        return df

    try:
        # Convert to GeoDataFrame indexed by school_id_giga
        gdf = gpd.GeoDataFrame(
            pdf_valid.set_index("school_id_giga"),
            geometry=gpd.points_from_xy(pdf_valid["longitude"], pdf_valid["latitude"]),
            crs="EPSG:4326",
        )

        # Build distance graph using giga-spatial (geodesic distance)
        G = build_distance_graph(gdf, gdf, PROXIMITY_DUPLICATE_THRESHOLD_M)

        # Find connected components and partition with cliques
        connected_components = [
            G.subgraph(comp).copy() for comp in nx.connected_components(G)
        ]

        group_id = 0
        duplicate_map = {}

        for cc in connected_components:
            if len(cc) > 1:
                if _is_complete_graph(cc):
                    for node in cc.nodes():
                        duplicate_map[node] = group_id
                    group_id += 1
                else:
                    cliques = _partition_graph_by_max_cliques(cc)
                    for clique in cliques:
                        if len(clique) > 1:
                            for node in clique:
                                duplicate_map[node] = group_id
                            group_id += 1

        # Graph degree = count of direct neighbors within threshold
        degree_dict = dict(G.degree())

        # Build result
        result_pdf = pdf.copy()
        for c in dup_cols:
            result_pdf[c] = None

        for idx in result_pdf.index:
            sid = result_pdf.at[idx, "school_id_giga"]
            if sid in duplicate_map:
                result_pdf.at[idx, "dq_duplicate_group_flag_50m"] = 1
                result_pdf.at[idx, "dq_duplicate_group_id_50m"] = duplicate_map[sid]
            elif sid in degree_dict:
                result_pdf.at[idx, "dq_duplicate_group_flag_50m"] = 0

            if sid in degree_dict:
                result_pdf.at[idx, "dq_duplicate_group_count_50m"] = degree_dict[sid]

        df = _join_pandas_result_to_spark(df, result_pdf, dup_cols)

        num_flagged = len(duplicate_map)
        num_groups = len(set(duplicate_map.values()))
        logger.info(
            f"Duplicate check complete: {num_flagged} schools in {num_groups} groups"
        )

    except Exception as e:
        logger.error(f"Proximity duplicate check failed: {e}")
        for c in dup_cols:
            df = df.withColumn(c, f.lit(None).cast("int"))

    for c in dup_cols:
        df = _null_guard_int(df, c)

    return df


def smod_classification(
    df: sql.DataFrame,
    country_code_iso3: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Classify schools as urban/rural using GHSL Settlement Model (SMOD) data
        via giga-spatial.

    Adds columns:
        - school_area_type_smod: SMOD label (e.g. "Urban Centre", "Rural Cluster")
        - rural_urban: ("Urban" or "Rural")
    """
    logger = get_context_with_fallback_logger(context)
    logger.info("Running SMOD urban/rural classification...")

    smod_cols_str = ["school_area_type_smod", "rural_urban"]

    try:
        pdf = _spark_to_pandas_coords(df)
        valid_mask = _get_valid_coords_mask(pdf)
        pdf_valid = pdf[valid_mask].copy()

        if pdf_valid.empty:
            logger.warning("No valid coordinates found for SMOD classification")
            for c in smod_cols_str:
                df = df.withColumn(c, f.lit(None).cast("string"))
            return df

        data_store = _get_data_store()
        view = PoiViewGenerator(
            points=pdf_valid,
            poi_id_column="school_id_giga",
            data_store=data_store,
        )
        result = view.map_smod(output_column="smod_class")

        if "smod_class" not in result.columns:
            logger.warning("No SMOD data available")
            for c in smod_cols_str:
                df = df.withColumn(c, f.lit(None).cast("string"))
            return df

        # Map SMOD numeric classes to human-readable labels and urban/rural
        result_pdf = pdf.copy()
        result_pdf["school_area_type_smod"] = None
        result_pdf["rural_urban"] = None

        smod_map = result.set_index("poi_id")["smod_class"].to_dict()
        for idx in result_pdf.index:
            sid = result_pdf.at[idx, "school_id_giga"]
            if sid in smod_map and pd.notna(smod_map[sid]):
                smod_val = int(smod_map[sid])
                result_pdf.at[idx, "school_area_type_smod"] = SMOD_CLASS_LABELS.get(
                    smod_val, f"Unknown ({smod_val})"
                )
                result_pdf.at[idx, "rural_urban"] = (
                    "Urban" if smod_val in SMOD_URBAN_CLASSES else "Rural"
                )

        df = _join_pandas_result_to_spark(df, result_pdf, smod_cols_str)

    except Exception as e:
        logger.error(f"SMOD classification failed: {e}")
        for c in smod_cols_str:
            df = df.withColumn(c, f.lit(None).cast("string"))

    for c in smod_cols_str:
        df = _null_guard_string(df, c)

    return df


def population_context_check(  # noqa
    df: sql.DataFrame,
    country_code_iso3: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Check and add population counts at multiple radii using WorldPop data
    Updates/adds columns: pop_within_1km, pop_within_2km, pop_within_3km, pop_within_5km, pop_within_10km
    """
    logger = get_context_with_fallback_logger(context)
    logger.info("Running population context check...")

    try:
        pdf = _spark_to_pandas_coords(df)
        valid_mask = _get_valid_coords_mask(pdf)
        pdf_valid = pdf[valid_mask].copy()

        if pdf_valid.empty:
            logger.warning("No valid coordinates found for population check")
            for col_name in POPULATION_RADII_M:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, f.lit(None).cast("long"))
            return df

        data_store = _get_data_store()
        view = PoiViewGenerator(
            points=pdf_valid,
            poi_id_column="school_id_giga",
            data_store=data_store,
        )

        for col_name, radius_m in POPULATION_RADII_M.items():
            logger.info(f"Computing {col_name} (radius={radius_m}m)...")
            try:
                view.map_wp_pop(
                    country=country_code_iso3,
                    map_radius_meters=radius_m,
                    output_column=col_name,
                )
            except Exception as e:
                logger.error(f"Population check failed for {col_name}: {e}")

        # Extract results from view
        view_df = view.view
        view_indexed = view_df.set_index("poi_id")
        pop_columns = list(POPULATION_RADII_M.keys())

        result_pdf = pdf.copy()
        for col_name in pop_columns:
            result_pdf[col_name] = None
            if col_name in view_indexed.columns:
                pop_map = view_indexed[col_name].to_dict()
                for idx in result_pdf.index:
                    sid = result_pdf.at[idx, "school_id_giga"]
                    if sid in pop_map and pd.notna(pop_map[sid]):
                        result_pdf.at[idx, col_name] = int(pop_map[sid])

        # Drop existing population columns from Spark DF before joining
        existing_pop_cols = [c for c in pop_columns if c in df.columns]
        if existing_pop_cols:
            df = df.drop(*existing_pop_cols)

        df = _join_pandas_result_to_spark(df, result_pdf, pop_columns)

        for col_name in pop_columns:
            df = df.withColumn(col_name, f.col(col_name).cast("long"))

    except Exception as e:
        logger.error(f"Population context check failed: {e}")
        for col_name in POPULATION_RADII_M:
            if col_name not in df.columns:
                df = df.withColumn(col_name, f.lit(None).cast("long"))

    # Ensure NULL for invalid coordinates
    for col_name in POPULATION_RADII_M:
        if col_name in df.columns:
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

    return df


def surrounding_schools_check(
    df: sql.DataFrame,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Count schools within various radii using giga-spatial's build_distance_graph.
    Adds columns:
        - schools_within_1km, schools_within_2km, schools_within_3km, schools_within_5km, schools_within_10km
    """
    logger = get_context_with_fallback_logger(context)
    logger.info("Running surrounding schools count...")

    col_names = [
        f"schools_within_{int(r / 1000)}km" for r in SURROUNDING_SCHOOLS_RADII_M
    ]

    pdf = _spark_to_pandas_coords(df)
    valid_mask = _get_valid_coords_mask(pdf)
    pdf_valid = pdf[valid_mask].copy()

    if pdf_valid.empty or len(pdf_valid) < 2:
        logger.warning("Not enough valid coordinates for surrounding schools check")
        for c in col_names:
            df = df.withColumn(c, f.lit(None).cast("int"))
        return df

    try:
        gdf = gpd.GeoDataFrame(
            pdf_valid.set_index("school_id_giga"),
            geometry=gpd.points_from_xy(pdf_valid["longitude"], pdf_valid["latitude"]),
            crs="EPSG:4326",
        )

        result_pdf = pdf.copy()

        for col_name, radius in zip(
            col_names, SURROUNDING_SCHOOLS_RADII_M, strict=False
        ):
            logger.info(f"Computing {col_name} (radius={radius}m)...")
            G = build_distance_graph(gdf, gdf, radius)
            neighbor_counts = {node: len(list(G.neighbors(node))) for node in G.nodes}
            result_pdf[col_name] = result_pdf["school_id_giga"].map(neighbor_counts)

        df = _join_pandas_result_to_spark(df, result_pdf, col_names)

    except Exception as e:
        logger.error(f"Surrounding schools check failed: {e}")
        for c in col_names:
            df = df.withColumn(c, f.lit(None).cast("int"))

    for c in col_names:
        df = _null_guard_int(df, c)

    return df


def run_geospatial_checks(
    df: sql.DataFrame,
    country_code_iso3: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    """Orchestrate all geospatial checks in the correct order.
    Single entry point that runs all geospatial DQ checks:
        1. Uninhabited area + suspect flag (buildings + built surface)
        2. Proximity duplicate detection (50m graph clustering + clique partitioning)
        3. SMOD urban/rural classification
        4. Population context (multi-radius WorldPop)
        5. Surrounding schools count (multi-radius)
    Returns:
        Spark DataFrame enriched with all geospatial DQ columns.
    """
    logger = get_context_with_fallback_logger(context)
    logger.info(f"Starting geospatial checks for country={country_code_iso3}...")
    logger.info(f"GEOSPATIAL_DATA_DIR={settings.GEOSPATIAL_DATA_DIR}")

    initial_count = df.count()
    logger.info(f"Input rows: {initial_count}")

    # 1. Uninhabited area + suspect flag
    df = uninhabited_area_check(df, country_code_iso3, context)

    # 2. Proximity duplicate detection
    df = proximity_duplicate_check(df, context)

    # 3. SMOD classification
    df = smod_classification(df, country_code_iso3, context)

    # 4. Population context
    df = population_context_check(df, country_code_iso3, context)

    # 5. Surrounding schools count
    df = surrounding_schools_check(df, context)

    final_count = df.count()
    logger.info(f"Geospatial checks complete. Rows: {initial_count} -> {final_count}")
    return df
