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

    await send_email_master_release_notification(props=props, recipients=recipients)

    slack_props = SlackProps(
        country=country,
        added=counts["added"],
        modified=counts["modified"],
        deleted=counts["deleted"],
        updateDate=update_date.strftime("%Y-%m-%d %H:%M:%S"),
        version=latest_version,
        rows=rows,
    )

    await send_slack_master_release_notification(props=slack_props)

    return {
        **props.dict(),
        "recipients": recipients,
    }
