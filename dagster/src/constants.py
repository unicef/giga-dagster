from pydantic import BaseSettings


class Constants(BaseSettings):
    raw_folder = "raw_dev"
    staging_approved_folder = "staging/approved"
    archive_manual_review_rejected_folder = "archive/manual-review-rejected"


constants = Constants()
