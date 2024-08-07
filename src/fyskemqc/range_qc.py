import numpy as np
import pandas as pd

from fyskemqc.base_qc_category import BaseQcCategory
from fyskemqc.qc_checks import RangeCheck
from fyskemqc.qc_flag import QcFlag
from fyskemqc.qc_flag_tuple import QcField


class RangeQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.RangeCheck, f"AUTO_QC_{QcField.RangeCheck}")

    def check(self, parameter: str, configuration: RangeCheck):
        selection = self._data.loc[self._data.parameter == parameter]
        self._data.loc[self._data.parameter == parameter, self._column_name] = np.where(
            pd.isna(selection.value),
            str(QcFlag.MISSING_VALUE.value),
            np.where(
                np.logical_and(
                    selection.value >= configuration.min_range_value,
                    selection.value <= configuration.max_range_value,
                ),
                str(QcFlag.GOOD_DATA.value),
                str(QcFlag.BAD_DATA.value),
            ),
        )
