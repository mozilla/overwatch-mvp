from typing import Iterable
from datetime import datetime

import click
import pytz

from analysis.detection.explorer.all_dimensions import AllDimensionEvaluator
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


def exceeds_top_level_percent_change(profile: AnalysisProfile, top_level_evaluation: dict) -> bool:
    top_level_percent_change = top_level_evaluation.get("top_level_percent_change")
    if abs(top_level_percent_change) <= profile.percent_change.overall_threshold_percent:
        logger.info(
            f"Absolute percent change of {top_level_percent_change} does not exceed threshold of"
            f" {profile.percent_change.overall_threshold_percent} for {profile.name} "
        )
        return False
    return True


# TODO GLE this function may be better inside the downstream processing.
def get_parent_df(top_level_evaluation: dict, top_level_dims_values_excluded_evaluation: dict):
    """
    Checks is there is a top level df with the dim values excluded to use in dependent
    processing.  If one does not exist then the top level df including all dimensions is used.
    @param top_level_evaluation:
    @param top_level_dims_values_excluded_evaluation:
    @return:
    """
    if len(top_level_dims_values_excluded_evaluation) == 0:
        return top_level_evaluation.get("top_level_values")
    else:
        return top_level_dims_values_excluded_evaluation.get(
            "top_level_values_dimension_values_excluded"
        )


def find_significant_dimensions(
    profile: AnalysisProfile,
    baseline_period: ProcessingDateRange,
    current_period: ProcessingDateRange,
) -> dict:
    # 1.  Find overall percent change
    # Perform top level calculation including all dimensions.
    evaluator = TopLevelEvaluator(
        profile=profile,
        baseline_period=baseline_period,
        current_period=current_period,
    )
    top_level_evaluation = evaluator.evaluate()
    logger.info(f"top_level_evaluation: {top_level_evaluation}")

    if not exceeds_top_level_percent_change(profile, top_level_evaluation):
        return {}

    # Calculate the top level values excluding all the specified dimension values.
    top_level_dims_values_excluded_evaluation = evaluator.evaluate_dimension_values_excluded()

    # Calculate the top level values including only the listed excluded values.
    top_level_excluded_dim_values_only_evaluation = (
        evaluator.evaluate_excluded_dimension_values_only()
    )

    # 2. Find
    # - percent change
    # - contribution to overall change for each value of single dimension.
    # - change to contribution of overall value for each value of single dimension.

    one_dim_evaluator = OneDimensionEvaluator(
        profile=profile,
        baseline_period=baseline_period,
        current_period=current_period,
        parent_df=get_parent_df(top_level_evaluation, top_level_dims_values_excluded_evaluation),
    )
    one_dim_evaluation = one_dim_evaluator.evaluate()

    multi_dim_evaluator = MultiDimensionEvaluator(
        profile=profile,
        baseline_period=baseline_period,
        current_period=current_period,
        parent_df=get_parent_df(top_level_evaluation, top_level_dims_values_excluded_evaluation),
    )
    multi_dim_evaluation = multi_dim_evaluator.evaluate()

    all_dim_evaluator = AllDimensionEvaluator(
        profile=profile,
        one_dim_evaluation=one_dim_evaluation,
        multi_dim_evaluation=multi_dim_evaluation,
    )

    all_dim_evaluation = all_dim_evaluator.evaluate()

    return (
        top_level_evaluation
        | top_level_dims_values_excluded_evaluation
        | top_level_excluded_dim_values_only_evaluation
        | one_dim_evaluation
        | multi_dim_evaluation
        | all_dim_evaluation
    )


def issue_report(
    profile: AnalysisProfile,
    notif_config: Notification,
    evaluation: dict,
    baseline_period: ProcessingDateRange,
    current_period: ProcessingDateRange,
):

    report_generator = ReportGenerator(
        output_dir="generated_reports",
        template=notif_config.report.template,
        analysis_profile=profile,
        notif_config=notif_config,
        evaluation=evaluation,
        baseline_period=baseline_period,
        current_period=current_period,
    )

    # Limited to publishing PDF to Slack for now.
    # In the future other notifications types will be supported.
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

                # TODO GLE removed since requires update to table schema.
                # insert_processing_info(
                #     config.analysis_profile,
                #     baseline_period,
                #     current_period,
                #     significant_dims.get("overall_change_calc"),
                # )

                # nothing significant found.
                if significant_dims == {}:
                    continue

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
