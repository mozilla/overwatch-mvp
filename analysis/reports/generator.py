import os
from datetime import datetime
from pathlib import Path

import pdfkit
from jinja2 import Environment, FileSystemLoader
from analysis.configuration.processing_dates import ProcessingDateRange

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
from adjustText import adjust_text

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
            + datetime.now().strftime("%Y-%m-%d")
        )
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        self.output_dir = output_dir
        self.filename_base = filename_base
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

    def build_pdf_charts(self):

        charts_output_dir = os.path.join(self.output_dir, "generated_charts")
        if not os.path.exists(charts_output_dir):
            os.makedirs(charts_output_dir)

        output_pdf = PdfPages(
            os.path.join(charts_output_dir, self.filename_base + "_charts" + ".pdf")
        )

        for dimension, df in self.evaluation["dimension_calc"].items():
            fig = self.build_plot(df.head(10), [dimension])
            output_pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

        for dimensions, df in self.evaluation["multi_dimension_calc"].items():
            fig = self.build_plot(df.head(10), list(dimensions))
            output_pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

        output_pdf.close()

    def build_plot(self, df, dimensions):
        fig = plt.figure(figsize=(10, 8))

        dimensions_str = " | ".join(dimensions)
        df[dimensions_str] = df["dimension_value_0"]

        for i in range(1, len(dimensions)):
            df[dimensions_str] = df[dimensions_str] + " | " + df["dimension_value_" + str(i)]

        sns.scatterplot(
            data=df,
            x="contrib_to_overall_change",
            y="change_to_contrib",
            hue=dimensions_str,
            size="percent_change",
            sizes=(40, 600),
        )

        # label data points wth dimension values
        texts = []
        for j in range(len(df[dimensions_str])):
            texts.append(
                plt.text(
                    df["contrib_to_overall_change"][j],
                    df["change_to_contrib"][j],
                    df.loc[j, dimensions_str],
                )
            )
        adjust_text(
            texts,
            only_move={"points": "y", "texts": "y"},
            arrowprops=dict(arrowstyle="->", color="r", lw=0.5),
        )

        plt.grid()
        plt.title(f"Dimension(s): {dimensions_str}")
        plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)

        # TODO:  save plots as png for adding to report
        # filename_base = self.filename_base + "_" + dimensions_str
        # output_png = os.path.join(self.output_dir, 'generated_charts', filename_base + ".png")
        # plt.savefig(output_png, bbox_inches='tight')

        return fig
