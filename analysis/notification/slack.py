import os

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from analysis.logging import logger


class SlackNotifier:
    def __init__(self, output_pdf, metric_name: str):
        self.output_pdf = output_pdf
        self.metric_name = metric_name

    def publish_pdf_report(
        self,
    ):
        try:
            client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
        except KeyError as e:
            # TODO GLE add slack channel name
            logger.error(
                "Environment variable `SLACK_BOT_TOKEN` is not assigned,"
                " unable to post report to slack channel"
            )
            raise e

        try:
            response = client.files_upload(
                channels="#data-monitoring-mvp",
                file=self.output_pdf,
                initial_comment=f"New Automated Analysis Report for {self.metric_name}",
            )
            assert response["file"]  # the uploaded file
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            logger.error(
                f'Unable to upload file to Slack channel, received: {e.response["status"]}'
            )
