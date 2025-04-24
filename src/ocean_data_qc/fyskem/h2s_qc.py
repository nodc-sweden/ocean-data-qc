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
        GOOD_DATA: H2S has flag bad or below detection or value isna
        BAD_DATA: all other H2S flags or value not isna
        BELOW_DETECTIONs: given parameter flag BELOW_DETECTION
        """

        parameter_boolean = self._data.parameter == parameter

        selection = self._data[parameter_boolean]
        other_selection = self._data[
            (self._data.parameter == "H2S")
            & ~self._data["quality_flag_long"].str.contains("(?:6|4)")
        ].rename(columns={"value": "h2s"})
        selection = pd.merge(
            selection,
            other_selection[["h2s", "visit_key", "DEPH"]],
            on=["visit_key", "DEPH"],
            how="left",
        )
        self._data.loc[parameter_boolean, self._column_name] = np.where(
            pd.isna(selection.value),  # no value means missing flag
            str(QcFlag.MISSING_VALUE.value),
            np.where(  # if not missing
                selection.quality_flag_long.str.contains(configuration.skip_flag),
                str(QcFlag.BELOW_DETECTION.value),  # keep Below detection
                np.where(  # if not below detection
                    pd.isna(selection.h2s),
                    str(QcFlag.GOOD_DATA.value),  # good when no h2s
                    str(QcFlag.BAD_DATA.value),  # bad when h2s
                ),
            ),
        )
