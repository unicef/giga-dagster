import asyncio
from datetime import datetime

from pydantic import BaseModel

from src.internal.groups import GroupsApi
from src.utils.email.send_email_base import send_email_base


class EmailProps(BaseModel):
    added: int
    country: str
    modified: int
    updateDate: datetime
    version: int
    rows: int


async def send_email_master_release_notification(country_code: str, props: EmailProps):
    members = await GroupsApi.list_country_members(country_code=country_code)
    recipients = [item.mail for item in members.values() if item.mail is not None]
    send_email_base(
        endpoint="email/master-data-release-notification",
        props=props,
        recipients=recipients,
        subject="Master Data Update Notification",
    )


if __name__ == "__main__":
    asyncio.run(send_email_master_release_notification())
