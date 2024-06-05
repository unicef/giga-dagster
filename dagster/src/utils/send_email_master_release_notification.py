from datetime import datetime

from loguru import logger
from pydantic import BaseModel, EmailStr

from src.settings import settings
from src.utils.email.send_email_base import send_email_base


class EmailProps(BaseModel):
    country: str
    added: int
    modified: int
    deleted: int
    updateDate: str
    version: int
    rows: int


async def send_email_master_release_notification(
    props: EmailProps, recipients: list[EmailStr]
):
    if len(recipients) == 0:
        logger.warning("No recipients found, skipping email.")
        return

    await send_email_base(
        endpoint="email/master-data-release-notification",
        props=props.dict(),
        recipients=recipients,
        subject="Master Data Update Notification",
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(
        send_email_master_release_notification(
            props=EmailProps(
                added=10,
                modified=50,
                deleted=3,
                country="PHL",
                updateDate=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                version=7,
                rows=1_000_912,
            ),
            recipients=[settings.ADMIN_EMAIL],
        )
    )
