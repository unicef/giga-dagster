from datetime import datetime

import country_converter as coco
from dagster_pyspark import PySparkResource
from pyspark import sql
from pyspark.sql import (
    SparkSession,
    functions as f,
)

from dagster import OpExecutionContext
from src.internal.groups import GroupsApi
from src.settings import DeploymentEnvironment, settings
from src.utils.op_config import FileConfig
from src.utils.send_email_master_release_notification import (
    EmailProps,
    send_email_master_release_notification,
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

    cdf = (
        s.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .table(f"school_master.{country_code}")
    )

    if cdf.count() == 0:
        context.log.warning("No changes to master, skipping email.")
        return None

    added = cdf.filter(f.col("_change_type") == "insert").count()
    modified = cdf.filter(f.col("_change_type") == "update_preimage").count()
    country = coco.convert(country_code, to="name_short")

    detail = s.sql(f"DESCRIBE DETAIL `school_master`.`{country_code}`").first()
    if detail is None:
        update_date = datetime.now()
    else:
        update_date = detail["lastModified"]

    history = s.sql(f"DESCRIBE HISTORY `school_master`.`{country_code}`")
    version = history.select(f.max("version").alias("version")).first()
    if version is None:
        version = 0
    else:
        version = version["version"]

    props = EmailProps(
        added=added,
        country=country,
        modified=modified,
        updateDate=update_date.strftime("%Y-%m-%d %H:%M:%S"),
        version=version,
        rows=rows,
    )

    if settings.DEPLOY_ENV == DeploymentEnvironment.LOCAL:
        recipients = [settings.ADMIN_EMAIL]
    elif settings.DEPLOY_ENV == DeploymentEnvironment.PRODUCTION:
        members = await GroupsApi.list_country_members(country_code=country_code)
        admins = await GroupsApi.list_group_members(group_name="Admin")
        recipients = {item.mail for item in members.values() if item.mail is not None}
        for admin in admins:
            if admin.mail is not None:
                recipients.add(admin.mail)
        recipients = list(recipients)
    else:
        members = await GroupsApi.list_group_members(group_name="Developer")
        recipients = list({m.mail for m in members if m.mail is not None})

    if len(recipients) == 0:
        context.log.warning(
            f"No recipients for country {country_code}, skipping email."
        )
        return None

    await send_email_master_release_notification(props=props, recipients=recipients)

    return {
        **props.dict(),
        "recipients": recipients,
    }
