from pathlib import Path
from typing import Union

import pandas as pd
import sharkadm
from sharkadm import sharkadm_exceptions

from fyskemqc import errors
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

    @classmethod
    def from_lims_data(cls, input_path: Path):
        try:
            dataframe = sharkadm.get_row_data_from_lims_export(input_path)
        except sharkadm_exceptions.DataHolderError:
            raise errors.InputDataError(f"Input data '{input_path}' could not be parsed.")

        return cls(dataframe)

    @classmethod
    def from_csv(cls, file_path: Union[Path, str]):
        try:
            data = pd.read_csv(file_path, sep="\t")
        except FileNotFoundError:
            raise errors.InputDataError(f"Input data '{file_path}' was not found.")

        return cls(data)
