# Dagster change: set `approval_status = "PENDING"` on file upload when staging completes

## Context

The `file_uploads` table in the primary Postgres DB now has a new nullable column:

```
approval_status: String  (nullable)
```

Possible values: `"PENDING"`, `"APPROVED"`, `"REJECTED"`, `null`.

- `null` — approval is not required for this upload (country/dataset combo has `ApprovalRequest.enabled = False`, or the upload type doesn't go through approval).
- `"PENDING"` — rows have been pushed to staging and are waiting for a reviewer to approve or reject them. **This is the value Dagster must set.**
- `"APPROVED"` / `"REJECTED"` — set by the API endpoint `POST /api/approval-requests/{country_code}/{upload_id}/submit` when the reviewer submits their decision.

## What the API already does

In `api/data_ingestion/routers/approval_requests.py`, the `submit_upload_review` endpoint now sets:

```python
if approved_change_ids:
    file_upload.approval_status = "APPROVED"
elif rejected_change_ids:
    file_upload.approval_status = "REJECTED"
```

This covers the transition from `PENDING` → `APPROVED` / `REJECTED`.

## What Dagster needs to do

When Dagster finishes pushing rows for an upload into the staging Delta Lake table (the step that currently sets `is_processed_in_staging = True` on the `file_uploads` record), it must also set `approval_status = "PENDING"` **if and only if** the `ApprovalRequest` for that country/dataset has `enabled = True`.

### Where to look in the Dagster repo

The relevant op/asset is wherever `is_processed_in_staging` is set to `True` on the `FileUpload` model. Search for:

```
is_processed_in_staging
```

That is the exact place to add the `approval_status` update.

### Logic to add

```python
from sqlalchemy import select
from data_ingestion.models.approval_requests import ApprovalRequest
from data_ingestion.models.file_upload import FileUpload

# After rows are successfully written to the staging Delta table:
approval_request = db.scalar(
    select(ApprovalRequest).where(
        ApprovalRequest.country == country_code.upper(),
        ApprovalRequest.dataset == formatted_dataset,  # e.g. "School Geolocation"
        ApprovalRequest.enabled == True,
    )
)

file_upload.is_processed_in_staging = True
if approval_request:
    file_upload.approval_status = "PENDING"

db.commit()
```

### Important details

- `formatted_dataset` follows the pattern `"School {dataset.capitalize()}"` (e.g. `"School Geolocation"`, `"School Coverage"`). This matches how `ApprovalRequest.dataset` is stored — see `approval_requests.py:448`.
- If `ApprovalRequest` does not exist or `enabled = False`, leave `approval_status` as `null`. Do not set it to `"PENDING"`.
- This must run inside the same DB transaction (or commit right after) as setting `is_processed_in_staging = True`, so both fields are always consistent.

## DB model reference

```python
# api/data_ingestion/models/file_upload.py
class FileUpload(BaseModel):
    ...
    is_processed_in_staging: Mapped[bool] = mapped_column(nullable=False, default=False)
    approval_status: Mapped[str] = mapped_column(nullable=True, default=None)
    ...
```

## Migration

The column is added in migration `c4e5f6a7b8c9` (`2026_04_25_1000_c4e5f6a7b8c9_add_mode_to_file_uploads.py`). Make sure the migration has been applied to the target environment before deploying the Dagster change.
