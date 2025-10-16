import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import IncreaseDecreaseCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class IncreaseDecreaseQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data,
            QcField.IncreaseDecrease,
            f"AUTO_QC_{QcField.IncreaseDecrease.name}",
        )

    def check(self, parameter: str, configuration: IncreaseDecreaseCheck):
        """
        check som kollar förändring från föregående djup av värdet på parameter
        GOOD_DATA: om förändringen ligger mellan allowed_increase och allowed_decrease
        BAD_DATA: om värdet på parameter utanför intervallet
        """
        self._parameter = parameter
        parameter_boolean = pl.col("parameter") == parameter

        # Early exit if nothing matches
        if self._data.filter(parameter_boolean).is_empty():
            return

        # Value column as reusable expression
        val = pl.col("value").fill_nan(None)

        # Difference per group (other parameters only)
        difference_expr = (
            val.diff()
            .over("visit_key")  # diff with nan as None
            .alias("difference")
        )

        # First value (normaly surface) will always be nan.
        selection = (
            self._data.filter(parameter_boolean)
            .sort(["visit_key", "DEPH"])
            .with_columns(difference_expr)
        )

        self._apply_flagging_logic(selection, configuration)

    def _apply_flagging_logic(
        self, selection: pl.DataFrame, configuration: IncreaseDecreaseCheck
    ) -> pl.DataFrame:
        """
        Apply flagging logic for value vs. summation deviation test using polars.
        """
        # Create the flag + info struct logic
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
            .when(
                (pl.col("difference") > -configuration.allowed_decrease)
                & (pl.col("difference") < configuration.allowed_increase)
                | pl.col("difference").is_null()
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD change from previous depth {} is within {}-{}",
                            pl.col("difference").round(2),
                            pl.lit(-configuration.allowed_decrease),
                            pl.lit(configuration.allowed_increase),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.format(
                            "BAD change from previous depth {} not within {}-{}",
                            pl.col("difference").round(2),
                            pl.lit(-configuration.allowed_decrease),
                            pl.lit(configuration.allowed_increase),
                        ).alias("info"),
                    ]
                )
            )
        )

        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)
