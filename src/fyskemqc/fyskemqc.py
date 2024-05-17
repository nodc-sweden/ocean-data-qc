import pandas as pd

from fyskemqc.detection_limit_qc import DetectionLimitQc
from fyskemqc.parameter import Parameter
from fyskemqc.qc_configuration import QcConfiguration
from fyskemqc.range_qc import RangeQc

QC_CATEGORIES = {
    "range_check": RangeQc,
    "detection_limit_check": DetectionLimitQc,
}


class FysKemQc:
    def __init__(self, data: pd.DataFrame):
        if "QC_FLAGS" not in data:
            data["QC_FLAGS"] = ""

        self._data = data
        self._configuration = QcConfiguration()

    def __len__(self):
        return len(self._data)

    def __getitem__(self, index):
        series = self._data.iloc[index]
        return Parameter(series, index)

    @property
    def parameters(self):
        return {Parameter(series, index) for index, series in self._data.iterrows()}

    def run_automatic_qc(self):
        for parameter in self.parameters:
            for category in self._configuration.categories:
                # Get config for parameter
                if config := self._configuration.get(category, parameter):
                    # Perform all checks
                    QC_CATEGORIES[category](config).check(parameter)

                    # Resync QC-flags with data
                    index, data = parameter.data
                    self._data.iloc[index] = data
