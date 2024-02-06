import requests
from src.settings import settings


def list_ipynb_from_github_repo(owner, repo, path):
    token = settings.GITHUB_ACCESS_TOKEN

    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"

    headers = {
        "Authorization": f"token {token}",
    }

    response = requests.get(api_url, headers=headers)

    ipynb_file_list = list()

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        content_list = response.json()

        # List ipynb files
        for content in content_list:
            if content["name"].endswith(".ipynb"):
                ipynb_file_list.append(
                    {
                        "filename": f"notebooks.{repo}.{content['name'].split('.')[0]}",
                        "url": content["html_url"],
                    }
                )
        return ipynb_file_list
    else:
        print(f"Failed to retrieve contents: {response.status_code}")


if __name__ == "__main__":
    owner = "unicef"
    repo = "coverage_workflow"
    path = "Notebooks/"
    print(list_ipynb_from_github_repo(owner, repo, path))
