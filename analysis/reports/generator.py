import os
from datetime import datetime
from pathlib import Path

import pdfkit
from jinja2 import Environment, FileSystemLoader
from analysis.configuration.processing_dates import ProcessingDateRange

import matplotlib.pyplot as plt
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
            (
                evaluation["profile"].dataset.metric_name
                + (
                    "_" + evaluation["profile"].dataset.app_name
                    if evaluation["profile"].dataset.app_name is not None
                    else ""
                )
                + "_"
                + datetime.now().strftime("%Y-%m-%d")
            )
            .replace(" ", "_")
            .replace("-", "_")
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

        abs_bar_plot_path = self.build_png_bar_plot()
        scatter_plot_paths = self.build_png_scatter_plots()

        with open(self.output_html, "w") as fh:
            fh.write(
                template.render(
                    evaluation=self.evaluation,
                    baseline_period=self.baseline_period,
                    current_period=self.current_period,
                    bar_plot_path=abs_bar_plot_path,
                    scatter_plot_paths=scatter_plot_paths,
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

        # clean png plots
        for filename in os.listdir(self.output_dir):
            file = os.path.join(self.output_dir, filename)
            if file.endswith("png"):
                os.remove(file)

        return self.output_pdf

    def build_png_scatter_plots(self):

        # TODO: only plotting top 10 results for now.
        #  Perhaps this could be configurable in the future.
        limit_results = 10
        absolute_paths = {}

        for dimension, df in self.evaluation["dimension_calc"].items():
            output_png = os.path.join(
                self.output_dir, self.filename_base + "_" + dimension + "_charts_scatter.png"
            )
            self.build_scatter_plot(df.head(limit_results), [dimension])
            plt.savefig(output_png, bbox_inches="tight")
            plt.close()

            abs_bar_plot_path = os.path.join(
                os.path.dirname(os.path.dirname(Path(os.path.dirname(__file__)))), output_png
            )
            absolute_paths[dimension] = abs_bar_plot_path

        for dimensions, df in self.evaluation["multi_dimension_calc"].items():
            dimension_str = "_".join(dimensions)
            output_png = os.path.join(
                self.output_dir, self.filename_base + "_" + dimension_str + "_charts_scatter.png"
            )
            self.build_scatter_plot(df.head(limit_results), list(dimensions))
            plt.savefig(output_png, bbox_inches="tight")
            plt.close()

            abs_bar_plot_path = os.path.join(
                os.path.dirname(os.path.dirname(Path(os.path.dirname(__file__)))), output_png
            )
            absolute_paths[dimension] = abs_bar_plot_path

        return absolute_paths

    def build_png_bar_plot(self):

        output_png = os.path.join(self.output_dir, self.filename_base + "_charts_bar.png")

        df = self.evaluation["overall_change_calc"].head(10)

        plt.figure(figsize=(5, 3))

        df["dimension_value(s)"] = df["dimension_value"] + "  (" + df["dimension"] + ")"

        sns.barplot(
            data=df,
            x="dimension_value(s)",
            y="change_distance",
            palette="Greys",
            hue="percent_change",
            dodge=False,
        )

        plt.xticks(rotation=90)
        plt.grid()
        plt.title("Overall Results")

        plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)

        plt.savefig(output_png, bbox_inches="tight")

        plt.close()

        return os.path.join(
            os.path.dirname(os.path.dirname(Path(os.path.dirname(__file__)))), output_png
        )

    def build_scatter_plot(self, df, dimensions):
        fig = plt.figure(figsize=(7.5, 6))

        dimensions_str = " | ".join(dimensions)

        df[dimensions_str] = df["dimension_value_0"]

        for i in range(1, len(dimensions)):
            df[dimensions_str] = df[dimensions_str] + " | " + df["dimension_value_" + str(i)]

        sns.scatterplot(
            data=df,
            x="contrib_to_overall_change",
            y="change_in_proportion",
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
                    df["change_in_proportion"][j],
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

        return fig
