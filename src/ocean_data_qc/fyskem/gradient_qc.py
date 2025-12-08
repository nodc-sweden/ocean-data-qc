import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import GradientCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class GradientQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data,
            QcField.Gradient,
            f"AUTO_QC_{QcField.Gradient.name}",
        )

    def check(self, parameter: str, configuration: GradientCheck):
        """
        Beräknar riktad gradient i parameter mellan två på varandra efterföljande djup.
        GOOD_DATA: om gradienten ligger mellan allowed_increase och allowed_decrease
        BAD_DATA: om gradienten ligger utanför intervallet
        """
        self._parameter = parameter
        selection = (
            self._data.filter(
                (pl.col("parameter") == parameter) & pl.col("value").is_not_null()
            )
            .sort(["visit_key", "DEPH"])
            .with_columns(
                [
                    (
                        pl.col("value").diff().over("visit_key")
                        / pl.col("DEPH").diff().over("visit_key")
                    ).alias("gradient")
                ]
            )
        )

        if selection.is_empty():
            return

        result_expr = self._apply_flagging_logic(configuration=configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(self, configuration: GradientCheck) -> pl.DataFrame:
        """
        Apply flagging logic for gradient test using polars.
        """
        # Create the flag + info struct logic
        result_expr = (
            pl.when(
                (pl.col("gradient") >= configuration.allowed_decrease)
                & (pl.col("gradient") <= configuration.allowed_increase)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD change from previous depth {} is within {}-{}",
                            pl.col("gradient").round(2),
                            pl.lit(configuration.allowed_decrease),
                            pl.lit(configuration.allowed_increase),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (pl.col("gradient") < configuration.allowed_decrease)
                | (pl.col("gradient") > configuration.allowed_increase)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.format(
                            "BAD change from previous depth {} not within {}-{}",
                            pl.col("gradient").round(2),
                            pl.lit(configuration.allowed_decrease),
                            pl.lit(configuration.allowed_increase),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.NO_QC_PERFORMED.value)).alias("flag"),
                        pl.format(
                            "Gradient is {} e.g. first depth with value at visit",
                            pl.col("gradient").round(2),
                        ).alias("info"),
                    ]
                )
            )
        )
        return result_expr
