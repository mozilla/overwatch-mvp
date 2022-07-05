import os
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from xhtml2pdf import pisa

from analysis.detection.profile import Detection


class Generator:
    html_report_filename = "report.html"
    pdf_report_filename = "report.pdf"

    def __init__(self, working_dir):
        self.output_html = os.path.join(working_dir, self.html_report_filename)
        self.output_pdf = os.path.join(working_dir, self.pdf_report_filename)

    def build_html_report(self, analysis_details: Detection, evaluation: dict):
        analysis_details._creation_time = str(datetime.now())
        p = Path(__file__).parent / "templates"
        env = Environment(loader=FileSystemLoader(p))
        template = env.get_template("report.html.j2")
        with open(self.output_html, "w") as fh:
            fh.write(
                template.render(
                    analysis_details=analysis_details, evaluation=evaluation
                )
            )

    # requires the html file to be created, will create if not available
    # returns relative path of pdf file.
    def build_pdf_report(self, analysis_details: Detection, evaluation: dict) -> str:
        self.build_html_report(analysis_details, evaluation)

        source_html = open(self.output_html, "r")
        result_file = open(self.pdf_report_filename, "w+b")

        # TODO GLE check pisa_status.err
        pisa_status = pisa.CreatePDF(source_html, dest=result_file)
        result_file.close()
        if pisa_status.err:
            print(f"Unable to create pdf from html file: {source_html}")
        return self.pdf_report_filename

    def publish_html_report_to_gcs(self):
        pass
