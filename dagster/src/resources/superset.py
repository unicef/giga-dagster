import os
import time

import requests
from nocodb import NocoDB
from requests.exceptions import ConnectionError, Timeout

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
    url = f"{SUPERSET_URL}/api/v1/security/login"
    login_payload = {
        "username": USERNAME,  # Replace with your Superset username
        "password": PASSWORD,  # Replace with your Superset password
        "provider": "db",  # Authentication type ('db' for database auth)
        "refresh": True,  # Ensures you get a refreshable access token
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=login_payload, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        auth_data = {
            "error": True,
            "status_code": response.status_code,
            "response_text": response.text,
        }
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
    timeout = 600
    max_retries = 3
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    sql_payload = {"database_id": DATABASE_ID, "sql": query["sql"]}
    url = f"{SUPERSET_URL}/api/v1/sqllab/execute/"

    start_time = time.time()
    attempt = 0
    while attempt < max_retries:
        try:
            print(f"Running query: {query['label']}")
            response = requests.post(
                url, json=sql_payload, headers=headers, timeout=timeout
            )
            duration = time.time() - start_time
            return {
                "status_code": response.status_code,
                "response_text": response.text,
                "duration": duration,
                "attempts": attempt + 1,
            }
        except (ConnectionError, Timeout) as e:
            attempt += 1
            print(f"[Attempt {attempt}] Connection error: {e}")
            if attempt >= max_retries:
                duration = time.time() - start_time
                return {
                    "status_code": "error",
                    "response_text": str(e),
                    "duration": duration,
                    "attempts": attempt,
                    "error": True,
                }
            time.sleep(10 * attempt)
