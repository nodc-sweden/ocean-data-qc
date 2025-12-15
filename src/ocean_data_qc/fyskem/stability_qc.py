import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import StabilityCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class StabilityQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data,
            QcField.Stability,
            f"AUTO_QC_{QcField.Stability.name}",
        )

    def check(self, parameter: str, configuration: StabilityCheck):
        """
        Beräknar förändring i densitet mellan två på varandra följande djup
        GOOD_DATA: om förändring ligger mellan allowed_increase och allowed_decrease
        BAD_DATA: om förändring ligger utanför intervallet
        """
        # self._parameter = parameter
        # parameter_boolean = pl.col("parameter") == parameter
        #

        self._parameter = parameter
        selection = (
            self._data.filter(
                (pl.col("parameter") == parameter) & pl.col("value").is_not_null()
            )
            .sort(["visit_key", "DEPH"])
            .with_columns(
                [(pl.col("value").diff().over("visit_key")).alias("difference")]
            )
        )
        if selection.is_empty():
            return

        result_expr = self._apply_flagging_logic(configuration=configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(self, configuration: StabilityCheck) -> pl.DataFrame:
        """
        Apply flagging logic for stability test using polars.
        """
        # Create the flag + info struct logic
        result_expr = (
            pl.when((pl.col("difference") < configuration.bad_decrease))
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "BAD, instable profile, decrease of {} is larger than the "
                            "allowed limit {} kg/m3",
                            pl.col("difference").round(4),
                            pl.lit(configuration.bad_decrease),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (pl.col("difference") < configuration.probably_bad_decrease)
                & (pl.col("difference") >= configuration.bad_decrease)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "PROBABLY BAD, instable profile, decrease of {} is between:"
                            "{} and {} kg/m3",
                            pl.col("difference").round(4),
                            pl.lit(configuration.probably_bad_decrease),
                            pl.lit(configuration.bad_decrease),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (pl.col("difference") < configuration.probably_good_decrease)
                & (pl.col("difference") >= configuration.probably_bad_decrease)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_GOOD_VALUE.value)).alias("flag"),
                        pl.format(
                            "PROBABLY GOOD, instable profile, decrease of {} is between:"
                            "{} and {} kg/m3",
                            pl.col("difference").round(4),
                            pl.lit(configuration.probably_good_decrease),
                            pl.lit(configuration.probably_bad_decrease),
                        ).alias("info"),
                    ]
                )
            )
            .when((pl.col("difference") >= configuration.probably_good_decrease))
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_VALUE.value)).alias("flag"),
                        pl.format(
                            "GOOD, stable profile, change of {} kg/m3 is acceptable",
                            pl.col("difference").round(4),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.NO_QUALITY_CONTROL.value)).alias("flag"),
                        pl.format(
                            "Difference is {} e.g. first depth with value at visit",
                            pl.col("difference").round(4),
                        ).alias("info"),
                    ]
                )
            )
        )
        return result_expr
