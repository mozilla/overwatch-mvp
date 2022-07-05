import os

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackNotifier:
    def __init__(self, output_pdf, metric_name: str):
        self.output_pdf = output_pdf
        self.metric_name = metric_name

    def publish_pdf_report(
        self,
    ):
        client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])

        try:
            response = client.files_upload(
                channels="#data-monitoring-mvp",
                file=self.output_pdf,
                initial_comment=f"New Automated Analysis Report for "
                f"{self.metric_name}",
            )
            assert response["file"]  # the uploaded file
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
