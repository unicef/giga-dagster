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

    if cdf.count() == 0:
        context.log.warning("No changes to master, skipping email.")
        return None

    column_changes_count = aggregate_changes_by_column_and_type(cdf)

    context.log.info(
        f"Preview of the column changes dataframe:\n{column_changes_count.show()}"
    )

    counts = get_change_operation_counts(cdf)
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
        **props.model_dump(),
        "recipients": recipients,
    }


def aggregate_changes_by_column_and_type(cdf: sql.DataFrame) -> sql.DataFrame:
    """
    Aggregate of the changes of each column for each change type including inserts, updates and deletions

    :param cdf: delta lake change data capture dataframe
    :return: DataFrame with columns: [column_name, operation, change_count]
    """

    # Filter update preimage and postimage
    preimage_df = cdf.filter(f.col("_change_type") == "update_preimage").alias(
        "preimage"
    )
    postimage_df = cdf.filter(f.col("_change_type") == "update_postimage").alias(
        "postimage"
    )

    pre_post_image_df = preimage_df.join(postimage_df, on="school_id_giga")
    master_data_cols = [
        column
        for column in cdf.columns
        if column not in ("_change_type", "_commit_version", "_commit_timestamp")
    ]

    # Updates
    update_changes_dfs = []
    for column in master_data_cols:
        has_changed_condition = f"preimage.{column} IS DISTINCT FROM postimage.{column}"
        update_changes_dfs.append(
            pre_post_image_df.filter(f.expr(has_changed_condition)).selectExpr(
                f"'{column}' as column_name", "'update' as change_type"
            )
        )

    # Inserts
    inserts_df = cdf.filter(f.col("_change_type") == "insert")
    insert_changes_dfs = []
    for column in master_data_cols:
        insert_changes_dfs.append(
            inserts_df.filter(f.col(column).isNotNull()).selectExpr(
                f"'{column}' as column_name", "'insert' as change_type"
            )
        )

    # Combine all the changes
    combined_changes_df = update_changes_dfs[0]
    for df_list in [update_changes_dfs[1:], insert_changes_dfs]:
        for df in df_list:
            combined_changes_df = combined_changes_df.union(df)

    # count of changes by column and change type
    column_changes_count = (
        combined_changes_df.groupBy("column_name", "change_type")
        .count()
        .withColumnsRenamed({"count": "change_count", "change_type": "operation"})
        .orderBy(f.col("change_count").desc())
    )

    return column_changes_count
