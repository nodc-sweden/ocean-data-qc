import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import StatisticCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class StatisticQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.Statistic, f"AUTO_QC_{QcField.Statistic.name}")

    def check(self, parameter: str, configuration: StatisticCheck):
        """Vectorized check that flags data based on sea_area, depth, and month."""

        self._parameter = parameter
        parameter_boolean = pl.col("parameter") == parameter

        # Early exit if nothing matches
        if self._data.filter(parameter_boolean).is_empty():
            return

        selection = (
            self._data.lazy()
            .filter(pl.col("parameter") == parameter)
            .with_columns(pl.col("visit_month").cast(pl.Int32))
            .join(
                configuration.data.lazy(),
                left_on=["sea_basin", "visit_month"],
                right_on=["sea_basin", "month"],
                how="left",
            )
            .filter(
                (pl.col("DEPH") >= pl.col("min_depth"))
                & (pl.col("DEPH") < pl.col("max_depth"))
            )
            .collect()
        )

        result_expr = self._apply_flagging_logic(configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection.clone(), result_expr=result_expr)

    def _apply_flagging_logic(
        self, configuration: StatisticCheck
    ) -> pl.DataFrame:
        """
        Apply flagging logic for delta (spike) check.
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
            .when(
                (
                    pl.col("flag1_lower").is_null()
                    | pl.col("flag1_upper").is_null()
                    | pl.col("flag2_lower").is_null()
                    | pl.col("flag2_upper").is_null()
                    | pl.col("flag3_lower").is_null()
                    | pl.col("flag3_upper").is_null()
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.NO_QC_PERFORMED.value)).alias("flag"),
                        pl.format(
                            "NO_QC_PERFORMED thresholds missing for {}",
                            pl.lit(self._parameter),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (pl.col("value") >= pl.col("flag1_lower"))
                & (pl.col("value") <= pl.col("flag1_upper"))
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD {} in [{}, {}]",
                            pl.col("value"),
                            pl.col("flag1_lower"),
                            pl.col("flag1_upper"),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (
                    (pl.col("value") > pl.col("flag2_lower"))
                    & (pl.col("value") < pl.col("flag1_lower"))
                )
                | (
                    (pl.col("value") > pl.col("flag1_upper"))
                    & (pl.col("value") < pl.col("flag2_upper"))
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "PROBABLY_GOOD: {} in range {}-{}]",
                            pl.col("value"),
                            pl.col("flag2_lower"),
                            pl.col("flag2_upper"),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (
                    (pl.col("value") >= pl.col("flag3_lower"))
                    & (pl.col("value") < pl.col("flag2_lower"))
                )
                | (
                    (pl.col("value") > pl.col("flag2_upper"))
                    & (pl.col("value") <= pl.col("flag3_upper"))
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA_CORRECTABLE.value)).alias("flag"),
                        pl.format(
                            "BAD_DATA_CORRECTABLE: {} in range {}-{}",
                            pl.col("value"),
                            pl.col("flag3_lower"),
                            pl.col("flag3_upper"),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.format(
                            "BAD {} outside range [{}, {}]",
                            pl.col("value"),
                            pl.col("flag3_lower"),
                            pl.col("flag3_upper"),
                        ).alias("info"),
                    ]
                )
            )
        )

        return result_expr
