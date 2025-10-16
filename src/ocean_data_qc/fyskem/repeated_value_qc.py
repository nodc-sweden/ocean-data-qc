import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import RepeatedValueCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class RepeatedValueQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data,
            QcField.RepeatedValue,
            f"AUTO_QC_{QcField.RepeatedValue.name}",
        )

    def check(self, parameter: str, configuration: RepeatedValueCheck):
        """
        This aims only to check for manual errors, therefore we look
        at the profile without blanks/none/nan.
        GOOD_DATA: first occurrence of a value
        PROBABLY_GOOD_DATA: repeated occurrence of a value
        """
        self._parameter = parameter
        parameter_boolean = pl.col("parameter") == parameter

        # Early exit if nothing matches
        if self._data.filter(parameter_boolean).is_empty():
            return

        # Difference per group (other parameters only)
        difference_expr = (
            pl.when(pl.col("value").is_not_null())
            .then(
                pl.col("value")
                - pl.col("value").fill_null(strategy="forward").shift(1).over("visit_key")
            )
            .otherwise(None)
            .alias("difference")
        )

        selection = (
            self._data.filter(parameter_boolean)
            .sort(["visit_key", "DEPH"])
            .with_columns(difference_expr)
        )

        self._apply_flagging_logic(selection, configuration)

    def _apply_flagging_logic(
        self, selection: pl.DataFrame, configuration: RepeatedValueCheck
    ) -> pl.DataFrame:
        """
        Apply flagging logic for repeated value test using polars.
        """

        # Create the flag + info struct logic
        result_expr = (
            pl.when(pl.col("value").is_null() | pl.col("value").is_nan())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.MISSING_VALUE.value)).alias("flag"),
                        pl.format(
                            "MISSING value not found for {}", pl.lit(self._parameter)
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (
                    (pl.col("difference") != configuration.repeated_value)
                    | pl.col("difference").is_null()
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD value",
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "PROBABLY GOOD value. The value is identical to the value "
                            "at the sampled depth above.",
                        ).alias("info"),
                    ]
                )
            )
        )

        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)
