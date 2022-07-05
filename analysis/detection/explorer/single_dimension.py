from datetime import datetime

from pandas import DataFrame

from analysis.detection.profile import AnalysisProfile, Detection


class SingleDimensionEvaluator:
    def __init__(self):
        pass

    def apply_profile(
        self,
        profile: AnalysisProfile,
        dimension: str,
        df: DataFrame,
        date_of_interest: datetime,
    ) -> Detection:
        pass
