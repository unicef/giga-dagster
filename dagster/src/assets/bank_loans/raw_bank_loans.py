import pandas as pd

from dagster import asset


@asset()
def raw__bank_loans():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "bank": ["Chase", "Wells Fargo", "Bank of America"],
            "loan_amount": [1000, 2000, 3000],
        }
    )
