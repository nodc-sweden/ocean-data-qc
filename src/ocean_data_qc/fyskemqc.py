import pandas as pd

from ocean_data_qc.fyskem.detection_limit_qc import DetectionLimitQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_configuration import QcConfiguration
from ocean_data_qc.fyskem.range_qc import RangeQc

QC_CATEGORIES = {
    "range_check": RangeQc,
    "detection_limit_check": DetectionLimitQc,
}


class FysKemQc:
    def __init__(self, data: pd.DataFrame):
        self._data = data
        self._configuration = QcConfiguration()

    def __len__(self):
        return len(self._data)

    def __getitem__(self, index):
        series = self._data.iloc[index]
        return Parameter(series)

    @property
    def parameters(self):
        return {Parameter(series) for _, series in self._data.iterrows()}

    def run_automatic_qc(self):
        for category in self._configuration.categories:
            # Get config for parameter
            category_checker = QC_CATEGORIES[category](self._data)
            category_checker.expand_qc_columns()

            for parameter in self._configuration.parameters(category):
                if config := self._configuration.get(category, parameter):
                    category_checker.check(parameter, config)

            category_checker.collapse_qc_columns()
