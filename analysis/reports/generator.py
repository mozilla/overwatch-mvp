import os
from datetime import datetime, timedelta
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from xhtml2pdf import pisa

from analysis.detection.profile import Detection


class Generator:
    def __init__(
        self, working_dir, template: str, evaluation: dict, analysis_details: Detection
    ):
        self.template = template
        self.output_html = os.path.join(
            working_dir, analysis_details.metric_name + ".html"
        )
        self.output_pdf = os.path.join(
            working_dir, analysis_details.metric_name + ".pdf"
        )
        self.analysis_details = analysis_details
        self.evaluation = evaluation

    def build_html_report(self):
        self.analysis_details._creation_time = str(datetime.now())
        p = Path(__file__).parent / "templates"
        env = Environment(loader=FileSystemLoader(p))
        template = env.get_template(self.template)
        with open(self.output_html, "w") as fh:
            fh.write(
                template.render(
                    analysis_details=self.analysis_details, evaluation=self.evaluation
                )
            )

    # requires the html file to be created, will create if not available
    # returns relative path of pdf file.
    def build_pdf_report(self) -> str:
        self.build_html_report()

        source_html = open(self.output_html, "r")
        result_file = open(self.output_pdf, "w+b")

        # TODO GLE check pisa_status.err
        pisa_status = pisa.CreatePDF(source_html, dest=result_file)
        result_file.close()
        if pisa_status.err:
            print(f"Unable to create pdf from html file: {source_html}")
        return self.output_pdf


# TODO GLE A lot more thought needs to be added to the report/notfication.
class PercentChangeGenerator:
    def __init__(self, working_dir, template: str, evaluation: dict, date_of_interest):
        self.template = template
        self.output_html = os.path.join(
            working_dir, evaluation["profile"].metric_name + ".html"
        )
        self.output_pdf = os.path.join(
            working_dir, evaluation["profile"].metric_name + ".pdf"
        )
        self.evaluation = evaluation
        self.date_of_interest = date_of_interest
        self.baseline_date = self.date_of_interest - timedelta(
            evaluation["profile"].historical_days_for_compare
        )

    def build_html_report(self):
        self.evaluation["creation_time"] = str(datetime.now())
        p = Path(__file__).parent / "templates"
        env = Environment(loader=FileSystemLoader(p))
        template = env.get_template(self.template)

        with open(self.output_html, "w") as fh:
            fh.write(
                template.render(
                    evaluation=self.evaluation,
                    date_of_interest=self.date_of_interest,
                    baseline_date=self.baseline_date,
                )
            )

    # requires the html file to be created, will create if not available
    # returns relative path of pdf file.
    def build_pdf_report(self) -> str:
        self.build_html_report()

        source_html = open(self.output_html, "r")
        result_file = open(self.output_pdf, "w+b")

        # TODO GLE check pisa_status.err
        pisa_status = pisa.CreatePDF(source_html, dest=result_file)
        result_file.close()
        if pisa_status.err:
            print(f"Unable to create pdf from html file: {source_html}")
        return self.output_pdf
