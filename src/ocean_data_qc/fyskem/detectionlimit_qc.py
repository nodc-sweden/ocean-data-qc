import numpy as np
import pandas as pd

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import DetectionLimitCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class DetectionLimitQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data, QcField.DetectionLimit, f"AUTO_QC_{QcField.DetectionLimit.name}"
        )

    def check(self, parameter, configuration: DetectionLimitCheck):
        """
        BELOW_DETECTION: everything
            - below detection limit
            and
            - at detection limit with incoming flag BELOW_DETECTION
        GOOD_DATA: everything above detection limit
        PROBABLY_GOOD_DATA: everything at detectionlimit without flag
        """
        selection = self._data.loc[self._data.parameter == parameter]
        self._data.loc[self._data.parameter == parameter, self._column_name] = np.where(
            pd.isna(selection.value),
            str(QcFlag.MISSING_VALUE.value),
            np.where(
                selection.value > configuration.limit,
                str(QcFlag.GOOD_DATA.value),
                np.where(
                    np.logical_and(
                        selection.value == configuration.limit,
                        ~selection.quality_flag_long.str.contains("6"),
                    ),
                    str(QcFlag.PROBABLY_GOOD_DATA.value),
                    str(QcFlag.BELOW_DETECTION.value),
                ),
            ),
        )
