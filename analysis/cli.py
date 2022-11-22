from typing import Iterable
from datetime import datetime

import click
import pytz

from analysis.detection.explorer.multiple_dimensions import MultiDimensionEvaluator
from analysis.detection.explorer.one_dimension import OneDimensionEvaluator
from analysis.detection.explorer.top_level import TopLevelEvaluator
from analysis.configuration.configs import AnalysisProfile, Notification
from analysis.logging import logger
from analysis.notification.slack import SlackNotifier
from analysis.reports.generator import ReportGenerator
from analysis.configuration.loader import Loader
from analysis.configuration.processing_dates import calculate_date_ranges, ProcessingDateRange


@click.group()
def cli():
    pass


def find_significant_dimensions(
    profile: AnalysisProfile,
    baseline_period: ProcessingDateRange,
    current_period: ProcessingDateRange,
) -> dict:
    # 1.  Find overall percent change
    top_level_evaluator = TopLevelEvaluator(
        profile=profile,
        baseline_period=baseline_period,
        current_period=current_period,
    )
    top_level_evaluation = top_level_evaluator.evaluate()
    logger.info(f"top_level_evaluation: {top_level_evaluation}")

    # 2. Find
    # - percent change
    # - contribution to overall change for each value of single dimension.
    # - change to contribution of overall value for each value of single dimension.

    one_dim_evaluator = OneDimensionEvaluator(
        profile=profile,
        baseline_period=baseline_period,
        current_period=current_period,
    )
    one_dim_evaluation = one_dim_evaluator.evaluate()

    multi_dim_evaluator = MultiDimensionEvaluator(
        profile=profile,
        baseline_period=baseline_period,
        current_period=current_period,
    )
    multi_dim_evaluation = multi_dim_evaluator.evaluate()

    return top_level_evaluation | one_dim_evaluation | multi_dim_evaluation


def issue_report(
    profile: AnalysisProfile,
    notif_config: Notification,
    evaluation: dict,
    baseline_period: ProcessingDateRange,
    current_period: ProcessingDateRange,
):
    evaluation["profile"] = profile

    report_generator = ReportGenerator(
        output_dir="generated_reports",
        template=notif_config.report.template,
        evaluation=evaluation,
        baseline_period=baseline_period,
        current_period=current_period,
    )

    pdfreport_filename = report_generator.build_pdf_report()
    # Only publish to Slack for MVP
    notifier = SlackNotifier(output_pdf=pdfreport_filename, config=notif_config.slack)
    notifier.publish_pdf_report()


class ClickDate(click.ParamType):
    """Converter for click date string parameters to datetime."""

    name = "date"

    def convert(self, value, param, ctx):
        """Convert a string to datetime."""
        if isinstance(value, datetime):
            return value
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=pytz.utc)


@cli.command()
@click.argument("paths", required=True, type=click.Path(exists=True, file_okay=True), nargs=-1)
@click.option(
    "--date",
    type=ClickDate(),
    help="Date for which projects should be analyzed",
    metavar="YYYY-MM-DD",
    required=True,
)
def run_analysis(paths: Iterable[str], date: ClickDate):
    logger.info(f"Starting analysis for date: {date} (excluded)")
    error_occurred = False
    for path in paths:
        configs = Loader.load_all_config_files(path)

        for config in configs:
            logger.info(f"Starting processing: {config.analysis_profile.name}")

            baseline_period, current_period = calculate_date_ranges(
                dataset_config=config.analysis_profile.dataset, exclusive_end_date=date
            )
            try:
                significant_dims = find_significant_dimensions(
                    profile=config.analysis_profile,
                    baseline_period=baseline_period,
                    current_period=current_period,
                )
                issue_report(
                    profile=config.analysis_profile,
                    evaluation=significant_dims,
                    baseline_period=baseline_period,
                    current_period=current_period,
                    notif_config=config.notification,
                )
                logger.info(f"Successfully processed: {config.analysis_profile.name}")
            except Exception:
                error_occurred = True
                logger.error(f"Error processing: {config.analysis_profile.name}", exc_info=1)

        logger.info(
            f"Analysis completed {'successfully' if not error_occurred else 'unsuccessfully'}."
        )

        if error_occurred:
            raise Exception("Processing error occurred.")


@cli.command()
@click.argument("paths", required=True, type=click.Path(exists=True, file_okay=True), nargs=-1)
def validate_config(paths: Iterable[str]):
    """
    Does not actually validate, only loads the config files.
    """
    logger.info(f"Validating config files in: {paths}")

    for path in paths:
        configs = Loader.load_all_config_files(path)
        logger.info(f"Loaded config: {configs} for path:{path}")


if __name__ == "__main__":
    cli()
