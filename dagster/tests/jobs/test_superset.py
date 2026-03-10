from unittest.mock import MagicMock, patch

from src.jobs.superset import fetch_and_run_query, post_query_durations_to_slack


def test_post_query_durations_to_slack():
    context = MagicMock()
    context.bind.return_value = context
    results = [{"title": "Q1", "duration": 1.23, "status_code": 200}]

    with (
        patch("src.jobs.superset.os.getenv") as mock_env,
        patch("src.jobs.superset.requests.post") as mock_post,
    ):
        mock_env.side_effect = (
            lambda k: "http://webhook" if k == "SLACK_WORKFLOW_WEBHOOK" else "stg"
        )
        mock_post.return_value.status_code = 200

        post_query_durations_to_slack(context=context, results=results)

        mock_post.assert_called_once()
        context.log.info.assert_called()


def test_fetch_and_run_query():
    context = MagicMock()
    context.bind.return_value = context

    with (
        patch("src.jobs.superset.get_access_token") as mock_get_token,
        patch("src.jobs.superset.fetch_saved_query") as mock_fetch,
        patch("src.jobs.superset.run_query") as mock_run,
    ):
        mock_get_token.return_value = {
            "access_token": "token",
            "refresh_token": "refresh",
        }
        mock_fetch.return_value = [
            {"Title": "T1", "Query to Delete": "DEL", "Query to Create": "CREATE"}
        ]
        mock_run.return_value = {"status_code": 200, "response_text": "OK"}

        with patch("src.jobs.superset.time.sleep"):
            results = fetch_and_run_query(context=context)

        assert len(results) == 1
        assert results[0]["title"] == "T1"
        assert mock_run.call_count == 2
