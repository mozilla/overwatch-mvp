import os
import logging
from datetime import datetime
from pathlib import Path

import pdfkit
from jinja2 import Environment, FileSystemLoader

# TODO GLE need to centralize config
logging.basicConfig(
    filename="overwatch.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)


# TODO GLE A lot more thought needs to be added to the report/notfication.
class ReportGenerator:
    def __init__(self, output_dir, template: str, evaluation: dict, date_ranges: dict):
        self.template = template
        self.input_path = Path(os.path.dirname(__file__))
        filename_base = (
            evaluation["profile"].metric_name
            + (
                "_" + evaluation["profile"].app_name
                if evaluation["profile"].app_name is not None
                else ""
            )
            + "_"
            + date_ranges.get("recent_period").get("end_date").strftime("%Y-%m-%d")
        )
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        self.output_html = os.path.join(output_dir, filename_base + ".html")
        self.output_pdf = os.path.join(output_dir, filename_base + ".pdf")
        self.evaluation = evaluation
        self.date_ranges = date_ranges

    def build_html_report(self):
        self.evaluation["creation_time"] = str(datetime.now())
        p = self.input_path / "templates"
        env = Environment(loader=FileSystemLoader(p))
        template = env.get_template(self.template)

        with open(self.output_html, "w") as fh:
            fh.write(
                template.render(
                    evaluation=self.evaluation,
                    date_ranges=self.date_ranges,
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
