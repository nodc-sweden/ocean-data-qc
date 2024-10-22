import numpy as np
import pandas as pd

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import ConsistencyCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class ConsistencyQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data, QcField.ConsistencyCheck, f"AUTO_QC_{QcField.ConsistencyCheck}"
        )

    def check(self, parameter: str, configuration: ConsistencyCheck):
        """
        This check is applied on the difference of the parameter value and the sum of the values in parameter list
        GOOD_DATA: difference >= 0
        PROBABLY_GOOD_DATA: lower_deviation <= difference <= upper_deviation
        BAD_DATA: everything outside the deviation bounds
        """  # noqa: E501
        selection = self._data.loc[self._data.parameter == parameter]

        if selection.empty:
            return

        other_selection = self._data.loc[
            self._data.parameter.isin(configuration.parameter_list)
        ].dropna(subset=["parameter"])

        if other_selection.empty:
            return

        summation = (
            other_selection.groupby(["visit_key", "DEPH"])["value"]
            .sum()
            .reset_index(name="summation")
        )
        selection = pd.merge(selection, summation, on=["visit_key", "DEPH"], how="left")

        self._data.loc[self._data.parameter == parameter, self._column_name] = np.where(
            pd.isna(selection.value),
            str(QcFlag.MISSING_VALUE.value),
            np.where(
                (selection.value - selection.summation) >= 0,
                str(QcFlag.GOOD_DATA.value),
                np.where(
                    np.logical_and(
                        (selection.value - selection.summation)
                        >= configuration.lower_deviation,
                        (selection.value - selection.summation)
                        <= configuration.upper_deviation,
                    ),
                    str(QcFlag.PROBABLY_GOOD_DATA.value),
                    str(QcFlag.BAD_DATA.value),
                ),
            ),
        )
