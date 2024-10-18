import numpy as np
import pandas as pd

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import TotalCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class TotalQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.TotalCheck, f"AUTO_QC_{QcField.TotalCheck}")

    def check(self, parameter: str, configuration: TotalCheck):
        selection = self._data.loc[self._data.parameter == parameter]

        if selection.empty:
            return

        other_selection = self._data.loc[
            self._data.parameter.isin(configuration.parameter_list)
        ]

        if other_selection.empty:
            return

        total = (
            other_selection.groupby(["visit_key", "DEPH"])["value"]
            .sum()
            .reset_index(name="total")
        )
        selection = pd.merge(selection, total, on=["visit_key", "DEPH"], how="left")

        self._data.loc[self._data.parameter == parameter, self._column_name] = np.where(
            pd.isna(selection.value),
            str(QcFlag.MISSING_VALUE.value),
            np.where(
                selection.value > selection.total,
                str(QcFlag.GOOD_DATA.value),
                str(QcFlag.BAD_DATA.value),
            ),
        )
