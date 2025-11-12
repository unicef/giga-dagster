import os
from typing import Any

import networkx as nx
import numpy as np
import pandas as pd
from gigaspatial.core.io import LocalDataStore
from gigaspatial.generators.poi import PoiViewGenerator, PoiViewGeneratorConfig
from gigaspatial.processing.algorithms import build_distance_graph
from networkx.algorithms.clique import find_cliques as maximal_cliques

from src.settings import settings


class GigaSpatialProcessor:
    """
    Utility class to calculate geochecks for school data using Giga Spatial lib.

    Provides methods to:
    - Calculate uninhabited areas based on building distances and surface area
    - Detect potential duplicate schools using graph-based clustering
    - Find surrounding schools within various distance thresholds
    """

    def __init__(self, context):
        """Initialize the GigaSpatialProcessor."""
        self.context = context
        self.surrounding_distances = [1000, 2000, 3000, 5000, 10000]
        self.urban_classes = [20, 21, 22, 23, 30]
        self.base_path = os.path.join(settings.BASE_DIR, "gigaspatial_data")
        self.config = PoiViewGeneratorConfig(base_path=self.base_path)
        self.data_store = LocalDataStore(base_path=self.base_path)

    def get_gigaspatial_calculated_cols(
        self,
        df: pd.DataFrame,
        country: str,
        meters: float = 50,
        building_threshold: float = 150,
        surface_threshold: float = 1,
    ) -> pd.DataFrame:
        """
        This is the core method that orchestrates all geospatial analyses:
        1. Generates spatial views using gigaspatial PoiViewGenerator
        2. Calculates uninhabited and suspect flags
        3. Adds urban/rural classification
        4. Detects potential duplicate schools
        5. Counts surrounding schools at various distances

        Args:
            df: DataFrame with school data
            country: Country Code
            meters: Distance threshold in meters for duplicate detection (default: 50)
            building_threshold: Distance threshold in meters for building proximity (default: 150)
            surface_threshold: Threshold in square meters for built surface area (default: 1)

        Returns:
            GeoDataFrame with original data plus calculated geochecks columns:
            - uninhabited: Boolean flag for uninhabited areas
            - suspect: Boolean flag for suspect areas
            - rural_urban: Urban/Rural classification (if SMOD data available)
            - duplicate_X_flag: Boolean flag for potential duplicates
            - duplicate_X_count: Count of nearby schools within threshold
            - duplicate_X_group_id: Group ID for duplicate clusters
            - schools_within_X_km: Count of schools within various distances
            - pop_within_Xkm: Population within various distances (if country data available)
        """
        surrounding_distances = self.surrounding_distances

        self.context.log.info(f"GigaSpatialProcessor processing {len(df)} schools")

        try:
            self.context.log.info("Generating spatial views with gigaspatial")
            view = PoiViewGenerator(
                points=df,
                config=self.config,
                data_store=self.data_store,
                logger=self.context.log,
            )

            # Map building data
            self._map_buildings_and_surfaces(view, building_threshold)

            try:
                view.map_smod()
                self.context.log.debug("Successfully mapped SMOD data")
            except Exception as e:
                self.context.log.warning(f"Failed to map SMOD: {str(e)}")

            if country:
                self.context.log.info(f"Mapping population data for country: {country}")
                for dist in surrounding_distances:
                    try:
                        view.map_wp_pop(
                            country=country,
                            map_radius_meters=dist,
                            output_column=f"pop_within_{int(dist/1000)}km",
                        )
                        self.context.log.debug(
                            f"Successfully mapped population data for {dist}m"
                        )
                    except Exception as e:
                        self.context.log.warning(
                            f"Failed to map population data for {dist}m: {str(e)}"
                        )

            gdf_view = view.to_geodataframe()
            self.context.log.debug(
                f"Generated view with {len(gdf_view)} rows and {len(gdf_view.columns)} columns"
            )

            self.context.log.info("Calculating uninhabited and suspect flags")
            gdf_geochecks = self.add_uninhabited(
                gdf_view, building_threshold, surface_threshold
            )

            # Add urban/rural classification if SMOD data is available
            if "smod_class" in gdf_geochecks.columns:
                self.context.log.info("Adding urban/rural classification")
                gdf_geochecks["smod_class"] = gdf_geochecks["smod_class"].astype(
                    "Int64"
                )
                gdf_geochecks["rurban"] = np.where(
                    gdf_geochecks["smod_class"].isin(self.urban_classes),
                    "Urban",
                    "Rural",
                )
                urban_count = (gdf_geochecks["rurban"] == "Urban").sum()
                self.context.log.debug(
                    f"Classified {urban_count} urban and {len(gdf_geochecks) - urban_count} rural schools"
                )

            # Set index for duplicate detection if school_id_giga exists
            if "school_id_giga" in gdf_geochecks.columns:
                gdf_geochecks.set_index("school_id_giga", inplace=True)
                self.context.log.debug(
                    "Set school_id_giga as index for duplicate detection"
                )

            # Detect duplicates
            self.context.log.info(
                f"Detecting potential duplicates with {meters}m threshold"
            )
            gdf_geochecks = self.assign_duplicate_group_ids(gdf_geochecks, meters)

            # Find surrounding schools
            self.context.log.info(
                f"Calculating surrounding school counts for distances: {surrounding_distances}m"
            )
            gdf_geochecks = self.find_surrounding_schools(
                gdf_geochecks, surrounding_distances=surrounding_distances
            )

            self.context.log.info(
                f"Successfully processed {len(gdf_geochecks)} schools with {len(gdf_geochecks.columns)} total columns"
            )
            return gdf_geochecks

        except Exception as e:
            self.context.log.error(
                f"Error in get_giga_spatial_calculated_cols: {str(e)}"
            )

    def _map_buildings_and_surfaces(self, view, building_threshold):
        """Attempt to map various building/surface sources"""
        # Google buildings
        try:
            view.map_google_buildings()
            self.context.log.debug("Successfully mapped Google buildings")
        except Exception as e:
            self.context.log.warning(f"Failed to map Google buildings: {str(e)}")

        # MS buildings
        try:
            view.map_ms_buildings()
            self.context.log.debug("Successfully mapped MS buildings")
        except (EOFError, Exception) as e:
            self.context.log.warning(
                f"Failed to map MS buildings: {str(e)}. Continuing without MS building data."
            )

        # built surface
        try:
            view.map_built_s(map_radius_meters=building_threshold)
            self.context.log.debug("Successfully mapped built surface data")
        except Exception as e:
            self.context.log.warning(f"Failed to map built surface: {str(e)}")

    def add_uninhabited(
        self,
        gdf_view: pd.DataFrame,
        building_threshold: float,
        surface_threshold: float,
    ) -> pd.DataFrame:
        """
        Add uninhabited and suspect flags based on building distances and surface area.
        Args:
            gdf_view: GeoDataFrame with school data
            building_threshold: Distance threshold in meters for building proximity
            surface_threshold: Threshold in square meters for built surface area
        Returns:
            GeoDataFrame with added 'uninhabited' and 'suspect' boolean columns
        """
        if gdf_view.empty:
            self.context.log.warning("Empty DataFrame provided to add_uninhabited")
            return gdf_view.copy()

        self.context.log.info(
            f"Adding uninhabited flags with building_threshold={building_threshold}, surface_threshold={surface_threshold}"
        )
        conditions = []
        gdf = gdf_view.copy()

        if "nearest_google_building_distance" in gdf.columns:
            conditions.append(
                gdf["nearest_google_building_distance"] > building_threshold
            )
            self.context.log.debug("Added Google building distance condition")

        if "nearest_ms_building_distance" in gdf.columns:
            conditions.append(gdf["nearest_ms_building_distance"] > building_threshold)
            self.context.log.debug("Added MS building distance condition")

        if "built_surface_m2" in gdf.columns:
            conditions.append(gdf["built_surface_m2"] < surface_threshold)
            self.context.log.debug("Added built surface condition")

        if conditions:
            combined_condition = np.logical_and.reduce(conditions)
            gdf["uninhabited"] = combined_condition
            combined_condition2 = np.logical_or.reduce(conditions)
            gdf["suspect"] = combined_condition2

            self.context.log.info(
                f"Found {combined_condition.sum()} uninhabited and {combined_condition2.sum()} suspect locations"
            )
        else:
            self.context.log.warning(
                "No relevant columns found for uninhabited analysis"
            )
            gdf["uninhabited"] = False
            gdf["suspect"] = False

        return gdf

    @staticmethod
    def _is_complete(G: nx.Graph) -> bool:
        """
        Check if a graph is complete (all nodes connected to all other nodes).
        Args:
            G: NetworkX graph
        Returns:
            True if graph is complete, False otherwise
        """
        n = len(G.nodes)
        m = len(G.edges)
        required_edges = n * (n - 1) // 2
        return m == required_edges

    @staticmethod
    def _partition_graph_by_max_cliques(G: nx.Graph) -> list[list[Any]]:
        """
        Partition a graph by finding maximal cliques with minimum weight.
        Args:
            G: NetworkX graph to partition
        Returns:
            List of cliques (each clique is a list of nodes)
        """

        def total_clique_weight(graph: nx.Graph, clique: list[Any]) -> float:
            """Calculate total weight of edges within a clique."""
            return sum(
                graph[u][v].get("weight", 1)
                for u in clique
                for v in clique
                if u < v and graph.has_edge(u, v)
            )

        G2 = G.copy()
        cliques = []

        while len(G2.nodes) > 0:
            all_cliques = list(maximal_cliques(G2))
            if not all_cliques:
                break

            max_size = max(len(c) for c in all_cliques)
            largest_cliques = [c for c in all_cliques if len(c) == max_size]
            chosen_clique = min(
                largest_cliques, key=lambda c: total_clique_weight(G2, c)
            )
            cliques.append(list(chosen_clique))
            G2.remove_nodes_from(chosen_clique)

        return cliques

    def assign_duplicate_group_ids(
        self, gdf: pd.DataFrame, threshold: float
    ) -> pd.DataFrame:
        """
        Assign duplicate group IDs to schools within a specified distance threshold.
        Args:
            gdf: GeoDataFrame with school data (must have geometry or lat/lon)
            threshold: Distance threshold in meters for considering schools as potential duplicates
        Returns:
            GeoDataFrame with added columns:
            - duplicate_{threshold}_group_id: Group ID for duplicate clusters (NaN if no duplicates)
            - duplicate_{threshold}_flag: Boolean flag indicating potential duplicates
        """
        if gdf.empty:
            self.context.log.warning(
                "Empty DataFrame provided to assign_duplicate_group_ids"
            )
            return gdf.copy()

        self.context.log.info(
            f"Detecting duplicates with threshold={threshold}m for {len(gdf)} schools"
        )

        try:
            # Build distance graph
            G = build_distance_graph(gdf, gdf, threshold)
            self.context.log.debug(
                f"Built distance graph with {len(G.nodes)} nodes and {len(G.edges)} edges"
            )

            # Get connected components
            connected_components = [
                G.subgraph(comp).copy() for comp in nx.connected_components(G)
            ]
            self.context.log.debug(
                f"Found {len(connected_components)} connected components"
            )

            # Process each connected component
            group_id = 0
            node_to_group = {}

            for cc in connected_components:
                if len(cc) > 1:  # Only process components with multiple nodes
                    if self._is_complete(cc):
                        # Complete graph: all nodes are duplicates of each other
                        nodes = list(cc.nodes())
                        for node in nodes:
                            node_to_group[node] = group_id
                        group_id += 1
                        self.context.log.debug(
                            f"Assigned complete group {group_id-1} with {len(nodes)} nodes"
                        )
                    else:
                        # Incomplete graph: use maximal cliques
                        cliques = self._partition_graph_by_max_cliques(cc)
                        for clique in cliques:
                            if len(clique) > 1:
                                for node in clique:
                                    node_to_group[node] = group_id
                                group_id += 1
                                self.context.log.debug(
                                    f"Assigned clique group {group_id-1} with {len(clique)} nodes"
                                )

            # Create result DataFrame
            gdf_new = gdf.copy()

            # Add group IDs
            col_group_id = f"duplicate_{int(threshold)}_group_id"
            col_flag = f"duplicate_{int(threshold)}_flag"
            col_count = f"duplicate_{int(threshold)}_count"

            gdf_new[col_group_id] = gdf_new.index.map(
                lambda x: node_to_group.get(x, -1)
            )
            gdf_new[col_flag] = gdf_new[col_group_id] >= 0
            gdf_new[col_group_id] = gdf_new[col_group_id].replace(-1, np.nan)

            # Add neighbor counts
            degree_dict = dict(G.degree())
            gdf_new[col_count] = gdf_new.index.map(degree_dict).fillna(0).astype(int)

            duplicate_count = gdf_new[col_flag].sum()
            self.context.log.info(
                f"Found {duplicate_count} schools with potential duplicates in {group_id} groups"
            )

            return gdf_new

        except Exception as e:
            self.context.log.error(f"Error in duplicate detection: {str(e)}")

    def find_surrounding_schools(
        self, gdf: pd.DataFrame, surrounding_distances: list[float] = None
    ) -> pd.DataFrame:
        """
        Find the number of schools within various distance thresholds for each school.
        Args:
            gdf: GeoDataFrame with school data (must have geometry or lat/lon)
            surrounding_distances: List of distance thresholds in meters.
        Returns:
            GeoDataFrame with added columns 'schools_within_X_km' for each threshold
        """
        if gdf.empty:
            self.context.log.warning(
                "Empty DataFrame provided to find_surrounding_schools"
            )
            return gdf.copy()

        self.context.log.info(
            f"Finding surrounding schools for {len(gdf)} schools at distances: {surrounding_distances}m"
        )

        gdf_result = gdf.copy()

        try:
            for threshold in surrounding_distances:
                self.context.log.debug(f"Processing threshold: {threshold}m")

                # Build distance graph for this threshold
                G = build_distance_graph(gdf, gdf, threshold)

                # Count neighbors for each node
                neighbor_counts = {
                    node: len(list(G.neighbors(node))) for node in G.nodes
                }

                # Add column
                col_name = f"schools_within_{int(threshold/1000)}_km"
                gdf_result[col_name] = (
                    gdf_result.index.map(neighbor_counts).fillna(0).astype(int)
                )

                total_neighbors = sum(neighbor_counts.values())
                self.context.log.debug(
                    f"Added {col_name}: total {total_neighbors} neighbor relationships"
                )

            self.context.log.info("Successfully calculated surrounding school counts")
            return gdf_result

        except Exception as e:
            self.context.log.error(f"Error in finding surrounding schools: {str(e)}")
