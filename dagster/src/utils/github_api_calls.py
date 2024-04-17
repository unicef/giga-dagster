from datetime import timedelta

import requests
from loguru import logger

from dagster import OpExecutionContext
from src.settings import settings
from src.utils.logger import get_context_with_fallback_logger


def list_ipynb_from_github_repo(
    owner: str,
    repo: str,
    path: str,
    context: OpExecutionContext = None,
) -> list[dict] | None:
    token = settings.GITHUB_ACCESS_TOKEN

    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"

    headers = {
        "Authorization": f"token {token}",
    }

    response = requests.get(
        api_url,
        headers=headers,
        timeout=int(timedelta(minutes=3).total_seconds()),
    )

    # Check if the request was successful
    if response.ok:
        # Parse the JSON response
        content_list = response.json()

        ipynb_file_list = [
            {
                "filename": f"notebooks.{repo}.{content['name'].split('.')[0]}",
                "url": content["html_url"],
                "file_size": f"{round(content['size']/1024,1)} KiB",
            }
            for content in content_list
            if content["name"].endswith(".ipynb")
        ]

        get_context_with_fallback_logger(context).info(ipynb_file_list)
        return ipynb_file_list

    get_context_with_fallback_logger(context).error(
        f"Failed to retrieve contents: Error {response.status_code}",
    )


if __name__ == "__main__":
    owner = "unicef"
    repo = "coverage_workflow"
    path = "Notebooks/"
    logger.info(list_ipynb_from_github_repo(owner, repo, path))
