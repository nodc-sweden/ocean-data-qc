import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import DetectionLimitCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class DetectionLimitQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data, QcField.DetectionLimit, f"AUTO_QC_{QcField.DetectionLimit.name}"
        )

    def check(self, parameter, configuration: DetectionLimitCheck):
        """
        BELOW_DETECTION: everything
            - below detection limit
            and
            - at detection limit with incoming flag BELOW_DETECTION
        GOOD_DATA: everything above detection limit
        PROBABLY_GOOD_DATA: everything at detectionlimit without flag
        """
        self._parameter = parameter
        parameter_boolean = pl.col("parameter") == parameter
        # Early exit if nothing matches
        if self._data.filter(parameter_boolean).is_empty():
            return

        selection = self._data.filter(parameter_boolean)
        result_expr = self._apply_flagging_logic(configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(self, configuration: DetectionLimitCheck) -> pl.DataFrame:
        """
        Apply flagging logic for value vs. summation deviation test using polars.
        """

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
            .when(pl.col("value") > configuration.limit)
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD value {} > detection limit {}",
                            pl.col("value").round(2),
                            pl.lit(configuration.limit),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (pl.col("value") == configuration.limit)
                & (pl.col("quality_flag_long").str.contains("1"))
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD value deliverer reported on detection limit {}",
                            pl.lit(configuration.limit),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BELOW_DETECTION.value)).alias("flag"),
                        pl.format(
                            "BELOW_DETECTION {} < {} or flagged as '6'",
                            pl.col("value"),
                            pl.lit(configuration.limit),
                        ).alias("info"),
                    ]
                )
            )
        )

        return result_expr
