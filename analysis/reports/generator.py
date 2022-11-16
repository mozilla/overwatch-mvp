import os
from datetime import datetime
from pathlib import Path

import pdfkit
from jinja2 import Environment, FileSystemLoader
from analysis.configuration.processing_dates import ProcessingDateRange


# TODO GLE A lot more thought needs to be added to the report/notfication.
class ReportGenerator:
    def __init__(
        self,
        output_dir,
        template: str,
        evaluation: dict,
        baseline_period: ProcessingDateRange,
        current_period: ProcessingDateRange,
    ):
        self.template = template
        self.input_path = Path(os.path.dirname(__file__))
        filename_base = (
            evaluation["profile"].dataset.metric_name
            + (
                "_" + evaluation["profile"].dataset.app_name
                if evaluation["profile"].dataset.app_name is not None
                else ""
            )
            + "_"
            + current_period.end_date.strftime("%Y-%m-%d")
        )
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        self.output_html = os.path.join(output_dir, filename_base + ".html")
        self.output_pdf = os.path.join(output_dir, filename_base + ".pdf")
        self.evaluation = evaluation
        self.baseline_period = baseline_period
        self.current_period = current_period

    def build_html_report(self):
        self.evaluation["creation_time"] = str(datetime.now())
        p = self.input_path / "templates"
        env = Environment(loader=FileSystemLoader(p))
        template = env.get_template(self.template)

        with open(self.output_html, "w") as fh:
            fh.write(
                template.render(
                    evaluation=self.evaluation,
                    baseline_period=self.baseline_period,
                    current_period=self.current_period,
                )
            )

    # requires the html file to be created, will create if not available
    # returns relative path of pdf file.
    def build_pdf_report(self) -> str:
        self.build_html_report()
        options = {"enable-local-file-access": None}
        css_file = str(self.input_path / "templates" / "4.3.1.bootstrap.min.css")

        pdfkit.from_file(
            self.output_html,
            self.output_pdf,
            options=options,
            css=css_file,
            verbose=True,
        )
        return self.output_pdf
