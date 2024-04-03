from dataclasses import dataclass, field

import pandas as pd

from fyskemqc.qc_flag import QcFlag


@dataclass
class QcObject:
    incoming: QcFlag = QcFlag.NO_QC_PERFORMED
    automatic: list[QcFlag] = field(default_factory=lambda: [QcFlag.NO_QC_PERFORMED])
    manual: QcFlag = QcFlag.NO_QC_PERFORMED

    def __str__(self):
        return (
            f"{self.incoming.value}_"
            f"{''.join(str(flag.value) for flag in self.automatic)}_"
            f"{self.manual.value}"
        )


class Parameter:
    def __init__(self, data: pd.Series):
        self._data = data
        self._qc = QcObject()

    @property
    def name(self):
        return self._data.parameter

    @property
    def value(self):
        return self._data.value

    @property
    def qc(self) -> QcObject:
        return self._qc

    @property
    def data(self):
        self._data.QC_FLAGS = str(self._qc)
        return self._data
