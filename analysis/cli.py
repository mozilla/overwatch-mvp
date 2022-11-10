from typing import Iterable
from datetime import datetime

import click

from analysis.detection.explorer.multiple_dimensions import MultiDimensionEvaluator
from analysis.detection.explorer.one_dimension import OneDimensionEvaluator
from analysis.detection.explorer.top_level import TopLevelEvaluator
from analysis.configuration.configs import AnalysisProfile, Notification
from analysis.logging import logger
from analysis.notification.slack import SlackNotifier
from analysis.reports.generator import ReportGenerator
from analysis.configuration.loader import Loader


@click.group()
def cli():
    pass


def find_significant_dimensions(profile: AnalysisProfile, date_ranges: dict) -> dict:
    # 1.  Find overall percent change
    top_level_evaluator = TopLevelEvaluator(profile=profile, date_ranges=date_ranges)
    top_level_evaluation = top_level_evaluator.evaluate()
    logger.info(f"top_level_evaluation: {top_level_evaluation}")

    # 2. Find
    # - percent change
    # - contribution to overall change for each value of single dimension.
    # - change to contribution of overall value for each value of single dimension.

    one_dim_evaluator = OneDimensionEvaluator(profile=profile, date_ranges=date_ranges)
    one_dim_evaluation = one_dim_evaluator.evaluate()

    multi_dim_evaluator = MultiDimensionEvaluator(profile=profile, date_ranges=date_ranges)
    multi_dim_evaluation = multi_dim_evaluator.evaluate()

    return top_level_evaluation | one_dim_evaluation | multi_dim_evaluation


def issue_report(
    profile: AnalysisProfile, notif_config: Notification, evaluation: dict, date_ranges: dict
):
    evaluation["profile"] = profile

    report_generator = ReportGenerator(
        output_dir="generated_reports",
        template=notif_config.report.template,
        evaluation=evaluation,
        date_ranges=date_ranges,
    )

    pdfreport_filename = report_generator.build_pdf_report()
    # TODO GLE hard coded to only publish to Slack for MVP
    notifier = SlackNotifier(output_pdf=pdfreport_filename, config=notif_config.slack)
    notifier.publish_pdf_report()


# This function is temporary until the date range config is added.
def _collect_time_ranges() -> dict[str, dict]:
    all_dates = {}
    new_profiles_fenix_date_ranges_of_interest = {
        "previous_period": {
            "start_date": datetime.strptime("2022-04-04", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-04-10", "%Y-%m-%d"),
        },
        "recent_period": {
            "start_date": datetime.strptime("2022-04-10", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-04-16", "%Y-%m-%d"),
        },
    }
    all_dates["Fenix new profiles"] = new_profiles_fenix_date_ranges_of_interest

    new_profiles_desktop_date_ranges_of_interest = {
        "previous_period": {
            "start_date": datetime.strptime("2022-05-01", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-05-01", "%Y-%m-%d"),
        },
        "recent_period": {
            "start_date": datetime.strptime("2022-06-30", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-06-30", "%Y-%m-%d"),
        },
    }
    all_dates["Desktop new profiles"] = new_profiles_desktop_date_ranges_of_interest

    dau_fenix_date_ranges_of_interest = {
        # 7 day configuration, dates are inclusive.  Current max window average is 7 days.
        # For single day configuration set start_date = end_date.
        "previous_period": {
            "start_date": datetime.strptime("2022-03-18", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-03-18", "%Y-%m-%d"),
        },
        "recent_period": {
            "start_date": datetime.strptime("2022-03-25", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-03-25", "%Y-%m-%d"),
        },
    }
    all_dates["Fenix DAU"] = dau_fenix_date_ranges_of_interest

    mau_firefox_desktop_date_ranges_of_interest = {
        # 7 day configuration, dates are inclusive.  Current max window average is 7 days.
        # For single day configuration set start_date = end_date.
        "previous_period": {
            "start_date": datetime.strptime("2021-11-02", "%Y-%m-%d"),
            "end_date": datetime.strptime("2021-11-02", "%Y-%m-%d"),
        },
        "recent_period": {
            "start_date": datetime.strptime("2021-11-30", "%Y-%m-%d"),
            "end_date": datetime.strptime("2021-11-30", "%Y-%m-%d"),
        },
    }
    all_dates["Desktop MAU"] = mau_firefox_desktop_date_ranges_of_interest

    download_date_ranges_of_interest = {
        # 7 day configuration, dates are inclusive.  Current max window average is 7 days.
        # For single day configuration set start_date = end_date.
        "previous_period": {
            "start_date": datetime.strptime("2022-07-30", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-07-30", "%Y-%m-%d"),
        },
        "recent_period": {
            "start_date": datetime.strptime("2022-08-06", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-08-06", "%Y-%m-%d"),
        },
    }

    all_dates["Site Metrics Downloads"] = download_date_ranges_of_interest
    return all_dates


@cli.command()
@click.argument("paths", required=True, type=click.Path(exists=True, file_okay=True), nargs=-1)
def run_analysis(paths: Iterable[str]):
    logger.info("Starting analysis")
    date_ranges = _collect_time_ranges()
    for path in paths:
        configs = Loader.load_all_config_files(path)

        # TODO GLE temp mapping of date range to Config
        #  will be removed once data range config completed.
        run_config = []
        for config in configs:
            run_config.append((config, date_ranges[config.analysis_profile.name]))

        for (config, date_ranges) in run_config:
            significant_dims = find_significant_dimensions(
                profile=config.analysis_profile, date_ranges=date_ranges
            )
            issue_report(
                profile=config.analysis_profile,
                evaluation=significant_dims,
                date_ranges=date_ranges,
                notif_config=config.notification,
            )
    logger.info("Analysis completed")


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
