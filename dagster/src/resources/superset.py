import requests
import os

SUPERSET_URL = "http://superset.ictd-ooi-superset-stg:8088"
USERNAME = os.getenv("SUPERSET_USERNAME")
PASSWORD = os.getenv("SUPERSET_PASSWORD")

def get_access_token():
    """Authenticate and return token"""
    url = f"{SUPERSET_URL}/api/v1/security/login"
    login_payload = {
        "username": USERNAME,  # Replace with your Superset username
        "password": PASSWORD,  # Replace with your Superset password
        "provider": "db",  # Authentication type ('db' for database auth)
        "refresh": True  # Ensures you get a refreshable access token
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(f"{SUPERSET_URL}/api/v1/security/login", json=login_payload, headers=headers)
    if response.status_code == 200:
        auth_data = response.json()
        access_token = auth_data["access_token"]
        refresh_token = auth_data["refresh_token"]  # If supported
        print("New Access Token:", access_token)
        print("Refresh Token:", refresh_token)
        return access_token
    else:
        print("Failed to authenticate:", response.status_code, response.text)
        return None

def get_saved_query(access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    saved_queries_url = f"{SUPERSET_URL}/api/v1/saved_query/"
    rison_filter = "(order_column:changed_on_delta_humanized,order_direction:asc,page_size:15,page:0)"
    params = {
        "q": rison_filter
    }
    response = requests.get(saved_queries_url, headers=headers, params=params)
    return response


def run_query(query, access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    sql_payload = {
        "database_id": 12,
        "sql": query["sql"]
    }

    print("running query", query["label"])

    response = requests.post(f"{SUPERSET_URL}/api/v1/sqllab/execute/", json=sql_payload, headers=headers)

    print("Status Code:", response.status_code)
    print("Response Text:", response.text)
