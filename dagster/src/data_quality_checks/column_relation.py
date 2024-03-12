from pyspark import sql
from pyspark.sql import functions as f

from dagster import OpExecutionContext
from src.utils.logger import get_context_with_fallback_logger


def column_relation_checks(
    df: sql.DataFrame, 
    dataset_type: str,
    context: OpExecutionContext = None,
) -> sql.DataFrame:
    
    logger = get_context_with_fallback_logger(context)
    logger.info("Starting column relation checks...")

    transforms = {}

    if dataset_type == "master":
        # connectivity = 'Yes' then (connectivity_RT or connectivity_govt or download_speed_contracted) is not NULL
            # expected behavior : yes -- yes|no|null|
            # expected behavior : no -- no&no&null = 0
            # expected behavior : no -- no&yes&null = 1
        transforms["dq_column_relation_checks-connectivity_connectivity_RT_connectivity_govt_download_speed_contracted"] = f.when(
                (f.lower(f.col("connectivity")) == "yes") & (
                    (f.lower(f.col("connectivity_RT")) == "yes") | 
                    (f.lower(f.col("connectivity_govt")) == "yes") |
                    (f.col("download_speed_contracted").isNotNull())
                    ), 0,
            ).when(
                (f.lower(f.col("connectivity")) == "no") & (
                    ((f.lower(f.col("connectivity_RT")) == "no") | f.col("connectivity_RT").isNull()) & 
                    ((f.lower(f.col("connectivity_govt")) == "no") | f.col("connectivity_govt").isNull()) &
                    (f.col("download_speed_contracted").isNull())
                    ), 0,
            ).otherwise(1)
    
        # connectivity_govt = yes download speed not null
        transforms["dq_column_relation_checks-connectivity_govt_download_speed_contracted"] = f.when(
                (f.col("download_speed_contracted").isNotNull()) &
                (f.col("connectivity_govt").isNull())
                , 1,
            ).otherwise(0)
    
        # Connectivity_RT -- connectivity_RT_datasource -- connectivity_RT_ingestion_timestamp if 1 present all are present
        transforms["dq_column_relation_checks-connectivity_RT_connectivity_RT_datasource_connectivity_RT_ingestion_timestamp"] =  f.when(
                    (f.col("connectivity_RT").isNull()) &
                    (f.col("connectivity_RT_datasource").isNull()) &
                    (f.col("connectivity_RT_ingestion_timestamp").isNull())
                    , 0,
                ).when(
                    (f.col("connectivity_RT").isNotNull()) &
                    (f.col("connectivity_RT_datasource").isNotNull()) &
                    (f.col("connectivity_RT_ingestion_timestamp").isNotNull())
                    , 0,
                ).otherwise(1)
    
        # cellular_coverage_availability -- cellular_coverage_type
        transforms["dq_column_relation_checks-cellular_coverage_availability_cellular_coverage_type"] = f.when(
                    (f.lower(f.col("cellular_coverage_availability")) == "yes") &
                    (f.lower(f.col("cellular_coverage_type")).isin(["2g", "3g", "4g", "5g"]))
                    , 0,
                ).when(
                    (f.lower(f.col("cellular_coverage_availability")) == "no") &
                    (f.lower(f.col("cellular_coverage_type")) == "no coverage")
                    , 0,
                ).when(
                    (f.col("cellular_coverage_availability").isNull()) &
                    (f.col("cellular_coverage_type").isNull())   
                    , 0,
                ).otherwise(1)

        # connectivity_govt -- connectivity_govt_ingestion_timestamp if connectivity_govt yes, timestamp isnotnull
        transforms["dq_column_relation_checks-connectivity_govt_connectivity_govt_ingestion_timestamp"] = f.when(
                    (f.lower(f.col("connectivity_govt")) == "yes") &
                    (f.col("connectivity_govt_ingestion_timestamp").isNull())
                    , 1,
                ).otherwise(0)

        # electricity_availability -- electricity_type if Electricity_availability yes, electricity_type isnotnull
        transforms["dq_column_relation_checks-electricity_availability_electricity_type"] = f.when(
                    (f.lower(f.col("electricity_availability")) == "yes") &
                    (f.col("electricity_type").isNull())
                    , 1,
                ).otherwise(0)

    elif dataset_type == "geolocation":
        # connectivity_govt = yes download speed not null
        transforms["dq_column_relation_checks-connectivity_govt_download_speed_contracted"] =  f.when(
                (f.col("download_speed_contracted").isNotNull()) &
                (f.col("connectivity_govt").isNull())
                , 1,
            ).otherwise(0)

        # electricity_availability -- electricity_type if Electricity_availability yes, electricity_type isnotnull
        transforms["dq_column_relation_checks-electricity_availability_electricity_type"] = f.when(
                    (f.lower(f.col("electricity_availability")) == "yes") &
                    (f.col("electricity_type").isNull())
                    , 1,
                ).otherwise(0)

    elif dataset_type == "QoS":
        transforms["dq_column_relation_checks-connectivity_RT_connectivity_RT_datasource_connectivity_RT_ingestion_timestamp"] = f.when(
                    (f.col("connectivity_RT").isNull()) &
                    (f.col("connectivity_RT_datasource").isNull()) &
                    (f.col("connectivity_RT_ingestion_timestamp").isNull())
                    , 0,
                ).when(
                    (f.col("connectivity_RT").isNotNull()) &
                    (f.col("connectivity_RT_datasource").isNotNull()) &
                    (f.col("connectivity_RT_ingestion_timestamp").isNotNull())
                    , 0,
                ).otherwise(1)
        
    elif dataset_type == "coverage":
        # nearest_XX_distance -- nearest_XX_id 
        column_pairs = {
            ("nearest_NR_id", "nearest_NR_distance"),
            ("nearest_LTE_id", "nearest_LTE_distance"),
            ("nearest_UMTS_id", "nearest_UMTS_distance"),
            ("nearest_GSM_id", "nearest_GSM_distance"),
        }

        for id, distance in column_pairs:
            transforms[f"dq_column_relation_checks-{id}_{distance}"] = f.when(
                        (f.col(f"{id}").isNull()) &
                        (f.col(f"{distance}").isNull())
                        , 0,
                    ).when(
                        (f.col(f"{id}").isNotNull()) &
                        (f.col(f"{distance}").isNotNull())
                        , 0,
                    ).otherwise(1)
            
    elif dataset_type == "coverage_itu":
        # nearest_XX_distance -- nearest_XX_id 
        column_pairs = {
            ("nearest_NR_id", "nearest_NR_distance"),
            ("nearest_LTE_id", "nearest_LTE_distance"),
            ("nearest_UMTS_id", "nearest_UMTS_distance"),
            ("nearest_GSM_id", "nearest_GSM_distance"),
        }

        for id, distance in column_pairs:
            transforms[f"dq_column_relation_checks-{id}_{distance}"] = f.when(
                        (f.col(f"{id}").isNull()) &
                        (f.col(f"{distance}").isNull())
                        , 0,
                    ).when(
                        (f.col(f"{id}").isNotNull()) &
                        (f.col(f"{distance}").isNotNull())
                        , 0,
                    ).otherwise(1)  
    else:
        pass 

    return df.withColumns(transforms)