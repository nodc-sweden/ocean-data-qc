import pandas as pd
import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import StatisticCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class StatisticQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data, QcField.StatisticCheck, f"AUTO_QC_{QcField.StatisticCheck.name}"
        )

    def check(self, parameter: str, configuration: StatisticCheck):
        """Vectorized check that flags data based on sea_area, depth, and month."""
        self._parameter = parameter
        # Filtrera ut rader för den aktuella parametern
        parameter_boolean = self._data.parameter == parameter
        selection = self._data.loc[parameter_boolean].copy()
        unique_combinations_config_input = selection[
            ["sea_basin", "DEPH", "visit_month"]
        ].drop_duplicates()
        thresholds = []
        for _, row in unique_combinations_config_input.iterrows():
            min_value, max_value = configuration.get_thresholds(
                row["sea_basin"], row["DEPH"], row["visit_month"]
            )
            thresholds.append((min_value, max_value))

        # Convert list to a DataFrame
        threshold_df = pd.DataFrame(
            thresholds, columns=["min_range_value", "max_range_value"]
        )
        unique_combinations_config_input = pd.concat(
            [unique_combinations_config_input.reset_index(drop=True), threshold_df],
            axis=1,
        )
        # Step 3: Merge back into the main dataframe
        selection = selection.merge(
            unique_combinations_config_input,
            on=["sea_basin", "DEPH", "visit_month"],
            how="left",
        )
        selection["min_range_value"] = pd.to_numeric(
            selection["min_range_value"], errors="coerce"
        )
        selection["max_range_value"] = pd.to_numeric(
            selection["max_range_value"], errors="coerce"
        )

        selection = self._apply_polars_flagging_logic(selection, configuration)
        self._data.loc[parameter_boolean, [self._column_name, self._info_column_name]] = (
            selection[[self._column_name, self._info_column_name]].values
        )

    def _apply_polars_flagging_logic(
        self, selection: pd.DataFrame, configuration: StatisticCheck
    ) -> pd.DataFrame:
        """
        Apply flagging logic for delta (spike) check.
        """
        pl_selection = pl.from_pandas(selection)

        result_expr = (
            pl.when(pl.col("value").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.MISSING_VALUE.value)).alias("flag"),
                        pl.format(
                            "MISSING no value for {}", pl.lit(self._parameter)
                        ).alias("info"),
                    ]
                )
            )
            .when(
                pl.col("min_range_value").is_null() | pl.col("max_range_value").is_null()
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.NO_QC_PERFORMED.value)).alias("flag"),
                        pl.format(
                            "NO_QC_PERFORMED min/max range missing for {}",
                            pl.lit(self._parameter),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (pl.col("value") >= pl.col("min_range_value"))
                & (pl.col("value") <= pl.col("max_range_value"))
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD {} in range {} - {}",
                            pl.col("value"),
                            pl.col("min_range_value"),
                            pl.col("max_range_value"),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.format(
                            "BAD {} outside range {} - {}",
                            pl.col("value"),
                            pl.col("min_range_value"),
                            pl.col("max_range_value"),
                        ).alias("info"),
                    ]
                )
            )
        )

        pl_selection = (
            pl_selection.with_columns([result_expr.alias("result_struct")])
            .with_columns(
                [
                    pl.col("result_struct").struct.field("flag").alias(self._column_name),
                    pl.col("result_struct")
                    .struct.field("info")
                    .alias(self._info_column_name),
                ]
            )
            .drop("result_struct")
        )

        return pl_selection.to_pandas()
