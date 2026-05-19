from datetime import datetime

import country_converter as coco
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)

from dagster import OpExecutionContext
from src.internal.groups import GroupsApi
from src.settings import DeploymentEnvironment, settings
from src.utils.delta import get_change_operation_counts
from src.utils.op_config import FileConfig
from src.utils.send_email_master_release_notification import (
    EmailProps,
    send_email_master_release_notification,
)
from src.utils.send_slack_master_release_notification import (
    SlackProps,
    format_changes_for_slack_message,
    send_slack_master_release_notification,
)


async def send_master_release_notes(
    context: OpExecutionContext,
    config: FileConfig,
    spark: PySparkResource,
    gold: sql.DataFrame,
) -> dict | None:
    rows = gold.count()
    if rows == 0:
        context.log.warning("No data in master, skipping email.")
        return None

    s: SparkSession = spark.spark_session
    country_code = config.country_code

    dt = DeltaTable.forName(s, f"school_master.{country_code}")
    latest_version = (dt.history().orderBy(f.col("version").desc()).first()).version
    if latest_version is None:
        latest_version = 0

    context.log.info(f"{latest_version=}")

    cdf = (
        s.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", latest_version)
        .table(f"school_master.{country_code}")
    )

    # Cache once — cdf is scanned by aggregate_changes_by_column_and_type,
    # get_change_operation_counts, and the count check below.
    cdf.cache()
    cdf_count = cdf.count()  # populates cache; reuse value instead of re-counting
    if cdf_count == 0:
        context.log.warning("No changes to master, skipping email.")
        cdf.unpersist()
        return None

    column_changes_count = aggregate_changes_by_column_and_type(cdf)
    context.log.info("Column changes computed.")

    counts = get_change_operation_counts(cdf)
    cdf.unpersist()
    country = coco.convert(country_code, to="name_short")

    detail = dt.detail().first()
    if detail is None:
        update_date = datetime.now()
    else:
        update_date = detail.lastModified

    props = EmailProps(
        country=country,
        added=counts["added"],
        modified=counts["modified"],
        deleted=counts["deleted"],
        updateDate=update_date.strftime("%Y-%m-%d %H:%M:%S"),
        version=latest_version,
        rows=rows,
    )

    if settings.DEPLOY_ENV == DeploymentEnvironment.LOCAL:
        recipients = [settings.ADMIN_EMAIL]
    elif settings.DEPLOY_ENV == DeploymentEnvironment.DEVELOPMENT:
        recipients = GroupsApi.list_role_members(role="Developer")
    else:
        members = GroupsApi.list_country_role_members(country_code)
        admins = GroupsApi.list_role_members("Admin")
        recipients = list({*members, *admins})

    if len(recipients) == 0:
        context.log.warning(
            f"No recipients for country {country_code}, skipping email."
        )
        return None

    await send_email_master_release_notification(
        props=props, recipients=recipients, context=context
    )

    column_changes_txt = format_changes_for_slack_message(column_changes_count)

    slack_props = SlackProps(
        country=country,
        added=counts["added"],
        modified=counts["modified"],
        deleted=counts["deleted"],
        updateDate=update_date.strftime("%Y-%m-%d %H:%M:%S"),
        version=latest_version,
        rows=rows,
        column_changes=column_changes_txt,
    )

    await send_slack_master_release_notification(props=slack_props)

    return {
        **props.dict(),
        "recipients": recipients,
    }


def aggregate_changes_by_column_and_type(cdf: sql.DataFrame) -> sql.DataFrame:
    """
    Aggregate of the changes of each column for each change type including inserts and updates.

    Uses array+explode instead of a per-column union chain so the Spark plan stays
    O(1) nodes regardless of schema width (~120 columns for geolocation).

    :param cdf: delta lake change data capture dataframe
    :return: DataFrame with columns: [column_name, operation, change_count]
    """
    master_data_cols = [
        c
        for c in cdf.columns
        if c not in ("_change_type", "_commit_version", "_commit_timestamp")
    ]

    preimage_df = cdf.filter(f.col("_change_type") == "update_preimage").alias(
        "preimage"
    )
    postimage_df = cdf.filter(f.col("_change_type") == "update_postimage").alias(
        "postimage"
    )
    pre_post_image_df = preimage_df.join(postimage_df, on="school_id_giga")

    # For each row: produce an array of column names where the value changed.
    # eqNullSafe is <=> (IS NOT DISTINCT FROM), so ~eqNullSafe is IS DISTINCT FROM.
    # when(...) returns the column name when changed, null otherwise;
    # array_compact removes the nulls; explode turns the array into one row per entry.
    update_changes = pre_post_image_df.select(
        f.explode(
            f.array_compact(
                f.array(
                    *[
                        f.when(
                            ~f.col(f"preimage.{c}").eqNullSafe(f.col(f"postimage.{c}")),
                            f.lit(c),
                        )
                        for c in master_data_cols
                    ]
                )
            )
        ).alias("column_name")
    ).withColumn("change_type", f.lit("update"))

    inserts_df = cdf.filter(f.col("_change_type") == "insert")

    # For each inserted row: produce an array of non-null column names.
    insert_changes = inserts_df.select(
        f.explode(
            f.array_compact(
                f.array(
                    *[f.when(f.col(c).isNotNull(), f.lit(c)) for c in master_data_cols]
                )
            )
        ).alias("column_name")
    ).withColumn("change_type", f.lit("insert"))

    return (
        update_changes.union(insert_changes)
        .groupBy("column_name", "change_type")
        .count()
        .withColumnsRenamed({"count": "change_count", "change_type": "operation"})
        .orderBy(f.col("change_count").desc())
    )
