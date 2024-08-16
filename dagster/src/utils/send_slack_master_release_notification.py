from datetime import datetime

from loguru import logger
from pydantic import BaseModel, EmailStr

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
async def send_slack_master_release_notification(
    props: SlackProps
):

    text = f"Country: {props.country}"

    if props.added > 0:
        text += f" Added: {props.added}"
    if props.modified > 0:
        text += f" Modified: {props.modified}"
    if props.deleted > 0:
        text += f" Deleted: {props.deleted}"

    text += f" Updated: {props.updateDate}"
    text += f" Version: {props.version}"
    text += f" Rows: {props.rows}"

    await send_slack_base(text)
