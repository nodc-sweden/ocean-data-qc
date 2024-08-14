import numpy as np
import pandas as pd

from fyskemqc.base_qc_category import BaseQcCategory
from fyskemqc.qc_checks import DetectionLimitCheck
from fyskemqc.qc_flag import QcFlag
from fyskemqc.qc_flag_tuple import QcField


class DetectionLimitQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data, QcField.DetectionLimitCheck, f"AUTO_QC_{QcField.DetectionLimitCheck}"
        )

    def check(self, parameter, configuration: DetectionLimitCheck):
        selection = self._data.loc[self._data.parameter == parameter]
        self._data.loc[self._data.parameter == parameter, self._column_name] = np.where(
            pd.isna(selection.value),
            str(QcFlag.MISSING_VALUE.value),
            np.where(
                selection.value > configuration.limit,
                str(QcFlag.GOOD_DATA.value),
                str(QcFlag.BELOW_DETECTION.value),
            ),
        )
