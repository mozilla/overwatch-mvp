import os

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from analysis.logging import logger
from analysis.configuration.configs import Slack


class SlackNotifier:
    def __init__(self, output_pdf, config: Slack):
        self.output_pdf = output_pdf
        self.channel = (
            os.environ.get("DEV_REPORT_SLACK_CHANNEL")
            if "DEV_REPORT_SLACK_CHANNEL" in os.environ
            else config.channel
        )
        self.message = config.message

    def publish_pdf_report(
        self,
    ):
        try:
            client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
        except KeyError as e:
            logger.error(
                "Environment variable `SLACK_BOT_TOKEN` is not assigned,"
                f" unable to post report to slack channel: {self.channel}"
            )
            raise e

        try:
            response = client.files_upload(
                channels=self.channel,
                file=self.output_pdf,
                initial_comment=self.message,
            )
            assert response["file"]  # the uploaded file
        except SlackApiError as e:
            # TODO GLE how to notify if cannot push to Slack?
            logger.error(f"Unable to upload file to Slack channel, received: {e.response}")
