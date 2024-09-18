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
    text += f"*Current Rows*: {props.rows}\n"

    await send_slack_base(text)
