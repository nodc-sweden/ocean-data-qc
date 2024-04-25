import pandas as pd

from fyskemqc.parameter import Parameter
from fyskemqc.qc_configuration import QcConfiguration
from fyskemqc.range_qc import RangeQc


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
            # Get config for parameter
            config = self._configuration.get(parameter)

            # Perform all checks
            RangeQc(config).check(parameter)

            # Resync QC-flags with data
            index, data = parameter.data
            self._data.iloc[index] = data
