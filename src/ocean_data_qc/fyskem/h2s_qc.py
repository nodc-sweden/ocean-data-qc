import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import H2sCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class H2sQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.H2s, f"AUTO_QC_{QcField.H2s.name}")

    def check(self, parameter: str, configuration: H2sCheck):
        """
        GOOD_DATA: H2S has flag bad or below detection or value isna
        BAD_DATA: all other H2S flags or value not isna
        BELOW_DETECTIONs: given parameter flag BELOW_DETECTION
        """

        self._parameter = parameter
        parameter_boolean = pl.col("parameter") == parameter

        # Early exit if nothing matches
        if self._data.filter(parameter_boolean).is_empty():
            return

        selection = self._data.filter(pl.col("parameter") == parameter).join(
            self._data.filter(
                (pl.col("parameter") == "H2S")
                & (~pl.col("quality_flag_long").str.contains(r"(?:6|4)"))
            ).select(
                [
                    pl.col("value").alias("h2s"),
                    pl.col("visit_key"),
                    pl.col("DEPH"),
                ]
            ),
            on=["visit_key", "DEPH"],
            how="left",
        )

        result_expr = self._apply_flagging_logic(configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(
        self, configuration: H2sCheck
    ) -> pl.DataFrame:
        """
        Apply the tests logic to selection
        """
        result_expr = (
            pl.when(pl.col("value").is_null() | pl.col("value").is_nan())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.MISSING_VALUE.value)).alias("flag"),
                        pl.lit(f"MISSING no value for {self._parameter}").alias("info"),
                    ]
                )
            )
            .when(pl.col("quality_flag_long").str.contains(configuration.skip_flag))
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BELOW_DETECTION.value)).alias("flag"),
                        pl.lit(
                            f"BELOW_DETECTION {self._parameter} is below detection limit"
                        ).alias("info"),
                    ]
                )
            )
            .when(pl.col("h2s").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.lit("GOOD no h2s present").alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.lit(f"BAD {self._parameter} because h2s present").alias(
                            "info"
                        ),
                    ]
                )
            )
        )

        return result_expr
