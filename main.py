from datetime import datetime

from analysis.detection.explorer.one_dimension import OneDimensionEvaluator
from analysis.detection.explorer.top_level import TopLevelEvaluator
from analysis.detection.profile import AnalysisProfile
from analysis.notification.slack import SlackNotifier
from analysis.reports.generator import ReportGenerator


def find_significant_dimensions(
    profile: AnalysisProfile, date_of_interest: datetime
) -> dict:
    # 1.  Find overall percent change
    top_level_evaluator = TopLevelEvaluator(
        profile=profile, date_of_interest=date_of_interest
    )
    top_level_evaluation = top_level_evaluator.evaluate()
    print(f"top_level_evaluation: {top_level_evaluation}")

    # 2. Find
    # - percent change
    # - contribution to overall change for each value of single dimension.
    # - change to contribution of overall value for each value of single dimension.

    one_dim_evaluator = OneDimensionEvaluator(
        profile=profile, date_of_interest=date_of_interest
    )
    one_dim_evaluation = one_dim_evaluator.evaluate()
    print(f"one_dim_evaluation: {one_dim_evaluation}")

    return top_level_evaluation | one_dim_evaluation


def issue_report(profile: AnalysisProfile, evaluation: dict, date_of_interest):
    evaluation["profile"] = profile

    report_generator = ReportGenerator(
        working_dir=".",
        template="report_version2.html.j2",
        evaluation=evaluation,
        date_of_interest=date_of_interest,
    )

    pdfreport_filename = report_generator.build_pdf_report()
    notifier = SlackNotifier(
        output_pdf=pdfreport_filename, metric_name=profile.metric_name
    )
    notifier.publish_pdf_report()


def run_poc():
    new_profiles_date_of_interest = datetime.strptime("2022-04-11", "%Y-%m-%d")

    new_profiles_ap = AnalysisProfile(
        # in this case threshold_percent is the threshold for contribution to overall change
        threshold_percent=1,
        metric_name="new_profiles",
        historical_days_for_compare=14,  # 14 days highlighted the new_profiles drop in early April
        dimensions=[
            "region_name",
            "subregion_name",
            "country",
            # "segment",  # not a good example
            # "channel",
            # "os",  # not a good example
            # "os_version",
            # "attribution_medium",
            # "attribution_source",
        ],
        sort_by=[
            "contrib_to_overall_change",
            "percent_change",
            "change_to_contrib",
            "percent_significance",
        ],
    )

    mau_date_of_interest = datetime.strptime("2021-11-30", "%Y-%m-%d")
    mau_ap = AnalysisProfile(
        # in this case threshold_percent is the threshold for contribution to overall change
        threshold_percent=1,
        metric_name="mau",
        historical_days_for_compare=28,
        dimensions=[
            "region_name",
            "subregion_name",
            "country",
            # "segment",  # not a good example
            # "channel",
            # "os",  # not a good example
            # "os_version",
            # "attribution_medium",
            # "attribution_source",
        ],
        sort_by=[
            "contrib_to_overall_change",
            "percent_change",
            "change_to_contrib",
            "percent_significance",
        ],
    )

    analysis_profiles = [
        (new_profiles_ap, new_profiles_date_of_interest),
        (mau_ap, mau_date_of_interest),
    ]

    for (profile, date) in analysis_profiles:
        significant_dims = find_significant_dimensions(
            profile=profile, date_of_interest=date
        )
        issue_report(
            profile=profile,
            evaluation=significant_dims,
            date_of_interest=date,
        )


if __name__ == "__main__":
    run_poc()
