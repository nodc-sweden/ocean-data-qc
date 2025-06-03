import pandas as pd
import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import RangeCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class RangeQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.RangeCheck, f"AUTO_QC_{QcField.RangeCheck.name}")

    def check(self, parameter: str, configuration: RangeCheck):
        self._parameter = parameter
        parameter_boolean = self._data.parameter == parameter
        selection = self._data.loc[parameter_boolean]

        selection = self._apply_polars_flagging_logic(selection, configuration)
        self._data.loc[parameter_boolean, [self._column_name, self._info_column_name]] = (
            selection[[self._column_name, self._info_column_name]].values
        )

    def _apply_polars_flagging_logic(
        self, selection: pd.DataFrame, configuration: RangeCheck
    ) -> pd.DataFrame:
        """
        Apply flagging logic for value vs. summation deviation test using polars.
        """
        pl_selection = pl.from_pandas(selection)
        min_val = float(configuration.min_range_value)
        max_val = float(configuration.max_range_value)

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
            .when((pl.col("value") >= min_val) & (pl.col("value") <= max_val))
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD {} in range {} - {}",
                            pl.col("value"),
                            pl.lit(min_val),
                            pl.lit(max_val),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.format(
                            "BAD {} out of range {} - {}",
                            pl.col("value"),
                            pl.lit(min_val),
                            pl.lit(max_val),
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
