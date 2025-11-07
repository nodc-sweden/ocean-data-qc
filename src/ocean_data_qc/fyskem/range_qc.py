import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import RangeCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class RangeQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.Range, f"AUTO_QC_{QcField.Range.name}")

    def check(self, parameter: str, configuration: RangeCheck):
        self._parameter = parameter
        parameter_boolean = pl.col("parameter") == parameter
        # Early exit if nothing matches
        if self._data.filter(parameter_boolean).is_empty():
            return
        selection = self._data.filter(parameter_boolean)
        result_expr = self._apply_flagging_logic(configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(
        self, configuration: RangeCheck
    ) -> pl.DataFrame:
        """
        Apply flagging logic for value vs. summation deviation test using polars.
        """
        min_val = float(configuration.min_range_value)
        max_val = float(configuration.max_range_value)

        result_expr = (
            pl.when(pl.col("value").is_null() | pl.col("value").is_nan())
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

        return result_expr
