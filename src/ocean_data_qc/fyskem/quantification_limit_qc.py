import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import QuantificationLimitCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class QuantificationLimitQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data,
            QcField.QuantificationLimit,
            f"AUTO_QC_{QcField.QuantificationLimit.name}",
        )

    def check(self, parameter, configuration: QuantificationLimitCheck):
        """
        BELOW_QUANTIFICATION:
            - everything below quantification limit
            and
            - at quantification limit without incoming flag or
            with flag BELOW_QUANTIFICATION
        GOOD_DATA:
            - everything above quantification limit
            and
            - at quantification limit with incoming flag GOOD_DATA
        """
        if "LMQNT_VAL" not in self._data.columns:
            self._data = self._data.with_columns(
                pl.lit(None).cast(pl.Float64).alias("LMQNT_VAL")
            )

        self._parameter = parameter
        selection = self._data.filter(
            (pl.col("parameter") == parameter) & pl.col("value").is_not_null()
        )

        if selection.is_empty():
            return

        result_expr = self._apply_flagging_logic(configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(
        self, configuration: QuantificationLimitCheck
    ) -> pl.DataFrame:
        """
        Apply flagging logic for value in comparison to given lmqnt limit.
        """

        result_expr = (
            pl.when(
                pl.col("LMQNT_VAL").is_not_null()
                & (pl.col("value") > pl.col("LMQNT_VAL"))
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_VALUE.value)).alias("flag"),
                        pl.format(
                            "GOOD value {} > quantification limit {}",
                            pl.col("value").round(3),
                            pl.col("LMQNT_VAL").round(3),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                pl.col("LMQNT_VAL").is_not_null()
                & (pl.col("value") == pl.col("LMQNT_VAL"))
                & (pl.col("INCOMING_QC") == QcFlag.GOOD_VALUE.value)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_VALUE.value)).alias("flag"),
                        pl.format(
                            "GOOD value {} > quantification limit {}",
                            pl.col("value").round(3),
                            pl.col("LMQNT_VAL").round(3),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (
                    pl.col("LMQNT_VAL").is_not_null()
                    & (pl.col("value") < pl.col("LMQNT_VAL"))
                )
                | (
                    pl.col("LMQNT_VAL").is_not_null()
                    & (pl.col("value") == pl.col("LMQNT_VAL"))
                    & (
                        pl.col("INCOMING_QC")
                        == QcFlag.VALUE_BELOW_LIMIT_OF_QUANTIFICATION.value
                    )
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(
                            str(QcFlag.VALUE_BELOW_LIMIT_OF_QUANTIFICATION.value)
                        ).alias("flag"),
                        pl.format(
                            "BELOW_QUANTIFICATION {} < {}, flagged as 'Q'",
                            pl.col("value").round(3),
                            pl.col("LMQNT_VAL").round(3),
                        ).alias("info"),
                    ]
                )
            )
            .when(pl.col("LMQNT_VAL").is_null() & (pl.col("value") > configuration.limit))
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_VALUE.value)).alias("flag"),
                        pl.format(
                            "GOOD value {} > quantification limit {}",
                            pl.col("value").round(2),
                            pl.lit(configuration.limit),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (
                    pl.col("LMQNT_VAL").is_null()
                    & (pl.col("value") == configuration.limit)
                    & (pl.col("INCOMING_QC") == QcFlag.GOOD_VALUE.value)
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_VALUE.value)).alias("flag"),
                        pl.format(
                            "GOOD value deliverer reported on quantification limit {}",
                            pl.lit(configuration.limit),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(
                            str(QcFlag.VALUE_BELOW_LIMIT_OF_QUANTIFICATION.value)
                        ).alias("flag"),
                        pl.format(
                            "BELOW_QUANTIFICATION {} < {} or flagged as 'Q'",
                            pl.col("value"),
                            pl.lit(configuration.limit),
                        ).alias("info"),
                    ]
                )
            )
        )

        return result_expr
