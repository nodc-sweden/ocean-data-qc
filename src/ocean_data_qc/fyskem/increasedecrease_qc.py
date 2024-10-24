import numpy as np
import pandas as pd

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import IncreaseDecreaseCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class IncreaseDecreaseQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data,
            QcField.IncreaseDecreaseCheck,
            f"AUTO_QC_{QcField.IncreaseDecreaseCheck}",
        )

    def check(self, parameter: str, configuration: IncreaseDecreaseCheck):
        """
        check som kollar förändring från föregående djup av värdet på parameter
        GOOD_DATA: om förändringen ligger mellan allowed_increase och allowed_decrease
        BAD_DATA: om värdet på parameter utanför intervallet
        """

        boolean_selection = self._data.parameter == parameter

        selection = self._data.loc[boolean_selection]

        # First value (normally surface) will always be nan.
        selection["difference"] = selection.groupby("visit_key")["value"].diff()

        self._data.loc[boolean_selection, self._column_name] = np.where(
            pd.isna(selection["value"]),
            str(QcFlag.MISSING_VALUE.value),
            np.where(
                np.logical_and(
                    selection.difference > -configuration.allowed_decrease,
                    selection.difference < configuration.allowed_increase,
                )
                | pd.isna(selection.difference),
                str(QcFlag.GOOD_DATA.value),
                str(QcFlag.BAD_DATA.value),
            ),
        )
