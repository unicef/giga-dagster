from datetime import datetime

import requests
from loguru import logger

from src.settings import settings


def list_ipynb_from_github_repo(owner: str, repo: str, path: str):
    token = settings.GITHUB_ACCESS_TOKEN

    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"

    headers = {
        "Authorization": f"token {token}",
    }

    response = requests.get(api_url, headers=headers)

    ipynb_file_list = []

    # Check if the request was successful
    if response.ok:
        # Parse the JSON response
        content_list = response.json()

        # List ipynb files
        for content in content_list:
            if content["name"].endswith(".ipynb"):
                # current date and time format: day/month/year 24-hour clock
                now = datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                ipynb_file_list.append(
                    {
                        "filename": f"notebooks.{repo}.{content['name'].split('.')[0]}",
                        "url": content["html_url"],
                        "file_size": f"{round(content['size']/1024,1)} KiB",
                        "last_ingestion_time": now,
                    }
                )
        return ipynb_file_list
    else:
        logger.error(f"Failed to retrieve contents: {response.status_code}")


if __name__ == "__main__":
    owner = "unicef"
    repo = "coverage_workflow"
    path = "Notebooks/"
    logger.info(list_ipynb_from_github_repo(owner, repo, path))
