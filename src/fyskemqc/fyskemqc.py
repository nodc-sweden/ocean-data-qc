from pathlib import Path
from typing import Union

import pandas as pd
import sharkadm
from sharkadm import sharkadm_exceptions

from fyskemqc import errors


class FysKemQc:
    def __init__(self, data: pd.DataFrame):
        self._data = data

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
