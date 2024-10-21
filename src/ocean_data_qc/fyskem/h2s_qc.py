import numpy as np
import pandas as pd

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import H2sCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class H2sQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.H2sCheck, f"AUTO_QC_{QcField.H2sCheck}")

    def check(self, parameter: str, configuration: H2sCheck):
        """
        BAD_DATA: H2S and given parameter not BELOW_DETECTION
        GOOD_DATA: all other cases
        no changes: if H2S or given parameter BELOW_DETECTION
        """

        parameter_boolean = (self._data.parameter == parameter) & ~self._data[
            "quality_flag_long"
        ].str.contains(configuration.skip_flag)

        selection = self._data[parameter_boolean]
        other_selection = self._data[
            (self._data.parameter == "H2S")
            & ~self._data["quality_flag_long"].str.contains(configuration.skip_flag)
        ].rename(columns={"value": "h2s"})
        selection = pd.merge(
            selection,
            other_selection[["h2s", "visit_key", "DEPH"]],
            on=["visit_key", "DEPH"],
            how="left",
        )

        self._data.loc[parameter_boolean, self._column_name] = np.where(
            pd.isna(selection.value),
            str(QcFlag.MISSING_VALUE.value),
            np.where(
                pd.isna(selection.h2s),
                str(QcFlag.GOOD_DATA.value),
                str(QcFlag.BAD_DATA.value),
            ),
        )
