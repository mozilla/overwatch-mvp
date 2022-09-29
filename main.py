from datetime import datetime
import logging 

from analysis.detection.explorer.multiple_dimensions import MultiDimensionEvaluator
from analysis.detection.explorer.one_dimension import OneDimensionEvaluator
from analysis.detection.explorer.top_level import TopLevelEvaluator
from analysis.detection.profile import AnalysisProfile
from analysis.notification.slack import SlackNotifier
from analysis.reports.generator import ReportGenerator

# TODO GLE centralize logging config
logging.basicConfig(filename='overwatch.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf-8',
    level=logging.INFO)


def find_significant_dimensions(profile: AnalysisProfile, date_ranges: dict) -> dict:
    # 1.  Find overall percent change
    top_level_evaluator = TopLevelEvaluator(profile=profile, date_ranges=date_ranges)
    top_level_evaluation = top_level_evaluator.evaluate()
    logging.info(f"top_level_evaluation: {top_level_evaluation}")

    # 2. Find
    # - percent change
    # - contribution to overall change for each value of single dimension.
    # - change to contribution of overall value for each value of single dimension.

    one_dim_evaluator = OneDimensionEvaluator(profile=profile, date_ranges=date_ranges)
    one_dim_evaluation = one_dim_evaluator.evaluate()

    multi_dim_evaluator = MultiDimensionEvaluator(
        profile=profile, date_ranges=date_ranges
    )
    multi_dim_evaluation = multi_dim_evaluator.evaluate()

    return top_level_evaluation | one_dim_evaluation | multi_dim_evaluation


def issue_report(profile: AnalysisProfile, evaluation: dict, date_ranges: dict):
    evaluation["profile"] = profile

    report_generator = ReportGenerator(
        output_dir="generated_reports",
        template="report_version2.html.j2",
        evaluation=evaluation,
        date_ranges=date_ranges,
    )

    pdfreport_filename = report_generator.build_pdf_report()
    notifier = SlackNotifier(
        output_pdf=pdfreport_filename, metric_name=profile.metric_name
    ) 
    notifier.publish_pdf_report()


def run_poc():
    # Analysis Example 2
    # Checking Fenix new profiles drop from Apr 16
    # https://docs.google.com/document/d/170I6yaaSws8LJEsMmXqDQ0U4IChVmRmyfgXmIhGUkrs/edit#
    new_profiles_fenix_date_ranges_of_interest = {
        # 7 day configuration, dates are inclusive.  Current max window average is 7 days.
        # For single day configuration set start_date = end_date.
        "previous_period": {
            "start_date": datetime.strptime("2022-04-04", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-04-10", "%Y-%m-%d"),
        },
        "recent_period": {
            "start_date": datetime.strptime("2022-04-10", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-04-16", "%Y-%m-%d"),
        },
    }
    new_profiles_fenix_ap = AnalysisProfile(
        # in this case threshold_percent is the threshold for contribution to overall change
        threshold_percent=1,
        metric_name="new_profiles",
        table_name="active_user_aggregates",
        app_name="Fenix",
        dimensions=[
            "region_name",
            "subregion_name",
            "country",
            "channel",
        ],
        sort_by=[
            "contrib_to_overall_change",
            "percent_change",
            "change_to_contrib",
            "percent_significance",
        ],
    )

    # Analysis Example 3
    # Checking increase in Firefox Desktop New Profiles from June 30 back 60 days
    new_profiles_desktop_date_ranges_of_interest = {
        # 7 day configuration, dates are inclusive.  Current max window average is 7 days.
        # For single day configuration set start_date = end_date.
        "previous_period": {
            "start_date": datetime.strptime("2022-05-01", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-05-01", "%Y-%m-%d"),
        },
        "recent_period": {
            "start_date": datetime.strptime("2022-06-30", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-06-30", "%Y-%m-%d"),
        },
    }

    new_profiles_desktop_ap = AnalysisProfile(
        # in this case threshold_percent is the threshold for contribution to overall change
        threshold_percent=0.25,
        metric_name="new_profiles",
        table_name="active_user_aggregates",
        app_name="Firefox Desktop",
        dimensions=[
            "region_name",
            "subregion_name",
            "country",
            "app_version",
        ],
        sort_by=[
            "contrib_to_overall_change",
            "percent_change",
            "change_to_contrib",
            "percent_significance",
        ],
    )

    # Analysis Example 1
    # Checking for increase in Firefox Desktop MAU from Nov 30.
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

    mau_firefox_desktop_ap = AnalysisProfile(
        # in this case threshold_percent is the threshold for contribution to overall change
        threshold_percent=1,
        metric_name="mau",
        app_name="Firefox Desktop",
        table_name="active_user_aggregates",
        dimensions=[
            "region_name",
            "subregion_name",
            "country",
        ],
        sort_by=[
            "contrib_to_overall_change",
            "percent_change",
            "change_to_contrib",
            "percent_significance",
        ],
    )

    # Analysis Example 4
    # Checking for drop in Fenix DAU mid March
    # https://docs.google.com/document/d/1umr5P35s4WM2zULyfNoBH_QrGubT9r89QeIuhE14Lzg/edit#
    dau_fenix_date_of_interest = datetime.strptime("2022-03-25", "%Y-%m-%d")
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

    dau_fenix_ap = AnalysisProfile(
        # in this case threshold_percent is the threshold for contribution to overall change
        threshold_percent=1,
        metric_name="dau",
        app_name="Fenix",
        table_name="active_user_aggregates",
        dimensions=[
            "region_name",
            "subregion_name",
            "country",
        ],
        sort_by=[
            "contrib_to_overall_change",
            "percent_change",
            "change_to_contrib",
            "percent_significance",
        ],
    )

    # Analysis Example 5
    # Download tracking
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
    download_ap = AnalysisProfile(
        # in this case threshold_percent is the threshold for contribution to overall change
        threshold_percent=1,
        metric_name="downloads",
        table_name="www_site_metrics_summary_v1",
        dimensions=[
            "country",
        ],
        sort_by=[
            "contrib_to_overall_change",
            "percent_change",
            "change_to_contrib",
            "percent_significance",
        ],
    )

    analysis_profiles = [
        (new_profiles_fenix_ap, new_profiles_fenix_date_ranges_of_interest),
        (new_profiles_desktop_ap, new_profiles_desktop_date_ranges_of_interest),
        (dau_fenix_ap, dau_fenix_date_ranges_of_interest),
        (mau_firefox_desktop_ap, mau_firefox_desktop_date_ranges_of_interest),
        (download_ap, download_date_ranges_of_interest)
    ]

    for (profile, date_ranges) in analysis_profiles:
        significant_dims = find_significant_dimensions(
            profile=profile, date_ranges=date_ranges
        )
        issue_report(
            profile=profile,
            evaluation=significant_dims,
            date_ranges=date_ranges,
        )


if __name__ == "__main__":
    run_poc()
