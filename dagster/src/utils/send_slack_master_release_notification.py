from pydantic import BaseModel

from src.settings import settings
from src.utils.slack.send_slack_base import send_slack_base


class SlackProps(BaseModel):
    country: str
    added: int
    modified: int
    deleted: int
    updateDate: str
    version: int
    rows: int
    column_changes: str


def format_changes_for_slack_message(df):
    rows = df.orderBy("operation", "column_name").collect()
    header = f"{'Column':<45} {'Operation':<12} {'Count':<6}"
    lines = [header, "-" * len(header)]
    for row in rows:
        lines.append(
            f"{row['column_name']:<45} {row['operation']:<12} {row['change_count']:<6}"
        )
    return "```\n" + "\n".join(lines) + "\n```"


async def send_slack_master_release_notification(props: SlackProps):
    text = f"*{props.country} Master Data Update* \n\n".upper()
    text += f"*Environment*: {settings.DEPLOY_ENV.upper()}\n"
    text += f"*Country*: {props.country}\n"

    if props.added > 0:
        text += f"*Added*: {props.added}\n"
    if props.modified > 0:
        text += f"*Modified*: {props.modified}\n"
    if props.deleted > 0:
        text += f"*Deleted*: {props.deleted}\n"

    text += f"*Updated*: {props.updateDate}\n"
    text += f"*Version*: {props.version}\n"
    text += f"*Current Rows*: {props.rows}\n\n\n"
    text += "*Column changes*\n\n"
    text += props.column_changes

    await send_slack_base(text)
