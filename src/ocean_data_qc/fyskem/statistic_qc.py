import numpy as np
import pandas as pd

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import StatisticCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class StatisticQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data, QcField.StatisticCheck, f"AUTO_QC_{QcField.StatisticCheck}"
        )

    def check(self, parameter: str, configuration: StatisticCheck):
        """Vectorized check that flags data based on sea_area, depth, and month."""
        # Filtrera ut rader för den aktuella parametern
        selection = self._data.loc[self._data.parameter == parameter].copy()
        print(selection.columns)
        unique_combinations_config_input = selection[
            ["sea_basin", "DEPH", "visit_month"]
        ].drop_duplicates()
        print(unique_combinations_config_input)
        thresholds = []
        for _, row in unique_combinations_config_input.iterrows():
            min_value, max_value = configuration.get_thresholds(
                row["sea_basin"], row["DEPH"], row["visit_month"]
            )
            thresholds.append((min_value, max_value))

        # Convert list to a DataFrame
        threshold_df = pd.DataFrame(thresholds, columns=["min_value", "max_value"])
        unique_combinations_config_input = pd.concat(
            [unique_combinations_config_input.reset_index(drop=True), threshold_df],
            axis=1,
        )
        print(threshold_df.head())
        # Step 3: Merge back into the main dataframe
        selection = selection.merge(
            unique_combinations_config_input,
            on=["sea_basin", "DEPH", "visit_month"],
            how="left",
        )
        pd.set_option("display.max_columns", 500)
        print(selection.head())
        # Använd np.where för att flagga data effektivt
        self._data.loc[self._data.parameter == parameter, self._column_name] = np.where(
            selection["value"].isna(),
            str(QcFlag.MISSING_VALUE.value),
            np.where(
                (selection["min_value"].isna() | selection["max_value"].isna()),
                str(QcFlag.NO_QC_PERFORMED.value),
                np.where(
                    (selection["value"] >= selection["min_value"])
                    & (selection["value"] <= selection["max_value"]),
                    str(QcFlag.GOOD_DATA.value),
                    str(QcFlag.BAD_DATA.value),
                ),
            ),
        )
