import os

import requests
from nocodb import NocoDB

SUPERSET_URL = os.getenv("SUPERSET_URL")
USERNAME = os.getenv("SUPERSET_USERNAME")
PASSWORD = os.getenv("SUPERSET_PASSWORD")
CATALOG_TOKEN = os.getenv("CATALOG_TOKEN")
CATALOG_BASE = os.getenv("CATALOG_BASE")

try:
    DATABASE_ID = int(os.getenv("DATABASE_ID"))
except Exception:
    DATABASE_ID = None


def get_access_token():
    """Authenticate and return token"""
    # url = f"{SUPERSET_URL}/api/v1/security/login"
    login_payload = {
        "username": USERNAME,  # Replace with your Superset username
        "password": PASSWORD,  # Replace with your Superset password
        "provider": "db",  # Authentication type ('db' for database auth)
        "refresh": True,  # Ensures you get a refreshable access token
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login", json=login_payload, headers=headers
    )
    auth_data = None
    if response.status_code == 200:
        auth_data = response.json()
    else:
        print("Failed to authenticate:", response.status_code, response.text)
    return auth_data


def refresh_access_token(refresh_token):
    headers = {"Authorization": f"Bearer {refresh_token}"}
    response = requests.post(f"{SUPERSET_URL}/api/v1/security/refresh", headers=headers)
    return response


def get_saved_query(access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    saved_queries_url = f"{SUPERSET_URL}/api/v1/saved_query/"
    rison_filter = "(order_column:changed_on_delta_humanized,order_direction:asc,page_size:15,page:0)"
    params = {"q": rison_filter}
    response = requests.get(saved_queries_url, headers=headers, params=params)
    return response


def fetch_saved_query():
    token = CATALOG_TOKEN
    base = CATALOG_BASE
    title = "Physical Table"
    noco = NocoDB(url="https://app.nocodb.com", api_key=token)

    base = noco.get_base(base)
    table = base.get_table_by_title(title)
    all_records = table.get_records()

    response = [record.get_values() for record in all_records]

    return response


def run_query(query, access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    sql_payload = {"database_id": DATABASE_ID, "sql": query["sql"]}

    print("running query", query["label"])

    response = requests.post(
        f"{SUPERSET_URL}/api/v1/sqllab/execute/", json=sql_payload, headers=headers
    )

    print("Status Code:", response.status_code)
    print("Response Text:", response.text)
    return response
