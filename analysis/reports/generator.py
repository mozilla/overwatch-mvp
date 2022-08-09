import os
from datetime import datetime, timedelta
from pathlib import Path

import pdfkit
from jinja2 import Environment, FileSystemLoader


# TODO GLE A lot more thought needs to be added to the report/notfication.
class ReportGenerator:
    def __init__(self, working_dir, template: str, evaluation: dict, date_of_interest):
        self.template = template
        self.output_html = os.path.join(
            working_dir,
            evaluation["profile"].metric_name
            + "_"
            + evaluation["profile"].app_name
            + date_of_interest.strftime("%Y-%m-%d")
            + ".html",
        )
        self.output_pdf = os.path.join(
            working_dir,
            evaluation["profile"].metric_name
            + "_"
            + evaluation["profile"].app_name
            + date_of_interest.strftime("%Y-%m-%d")
            + ".pdf",
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
        options = {"enable-local-file-access": None}
        pdfkit.from_file(
            self.output_html,
            self.output_pdf,
            options=options,
            css="./analysis/reports/templates/4.3.1.bootstrap.min.css",
            verbose=True,
        )
        return self.output_pdf
