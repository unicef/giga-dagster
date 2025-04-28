from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from dagster import Output
from src.constants import DataTier
from src.internal.merge import partial_in_cluster_merge
from src.spark.transform_functions import add_missing_columns
from src.utils.delta import check_table_exists
from src.utils.metadata import get_table_preview
from src.utils.schema import (
    construct_full_table_name,
    construct_schema_name_for_tier,
    get_primary_key,
    get_schema_columns,
)
from src.utils.spark import compute_row_hash, transform_types


def sync_silver_from_gold(
    context,
    spark,
    config,
    silver_schema: str,
):
    s: SparkSession = spark.spark_session
    code = config.country_code.lower()
    silver_hive = construct_schema_name_for_tier(silver_schema, DataTier.SILVER)
    silver_table = construct_full_table_name(silver_hive, code)

    if not check_table_exists(s, silver_schema, code, DataTier.SILVER):
        context.log.warning(f"No silver for {silver_schema}.{code}, skipping")
        return Output(None)
    existing_silver = DeltaTable.forName(s, silver_table).toDF()

    master = DeltaTable.forName(s, f"school_master.{code}").toDF().drop("signature")
    master = add_missing_columns(master, get_schema_columns(s, "school_master"))

    if check_table_exists(s, "school_reference", code, DataTier.GOLD):
        ref = DeltaTable.forName(s, f"school_reference.{code}").toDF().drop("signature")
        ref = add_missing_columns(ref, get_schema_columns(s, "school_reference"))
    else:
        empty = StructType(get_schema_columns(s, "school_reference"))
        ref = s.createDataFrame([], empty)

    gold = master.alias("m").join(ref.alias("r"), "school_id_giga", "left")
    silver_cols = [f.name for f in get_schema_columns(s, silver_schema)]
    incoming = gold.select(*[c for c in silver_cols if c != "signature"])
    incoming = add_missing_columns(incoming, get_schema_columns(s, silver_schema))
    incoming = transform_types(incoming, silver_schema, context)
    incoming = compute_row_hash(incoming, context)

    pk = get_primary_key(s, silver_schema)
    updated = partial_in_cluster_merge(existing_silver, incoming, pk, silver_cols)

    updated.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(silver_table)

    return Output(
        None,
        metadata={
            "row_count": updated.count(),
            "preview": get_table_preview(updated),
        },
    )
