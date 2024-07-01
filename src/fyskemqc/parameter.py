import pandas as pd

from fyskemqc.qc_flags import QcFlags


class Parameter:
    def __init__(self, data: pd.Series, index: int = None):
        self._index = index
        self._data = data
        if "quality_flag_long" in data:
            self._qc = QcFlags.from_string(data["quality_flag_long"])
        else:
            self._qc = QcFlags()

    @property
    def name(self):
        return self._data.parameter

    @property
    def value(self):
        return self._data.value

    @property
    def qc(self) -> QcFlags:
        return self._qc

    @property
    def data(self):
        self._data["quality_flag_long"] = str(self._qc)
        return self._index, self._data
