import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import SpikeCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class SpikeQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.Spike, f"AUTO_QC_{QcField.Spike.name}")

    def check(self, parameter: str, configuration: SpikeCheck):
        """
        check som kollar förändring relativt föregående djup och nästa djup.
        Förändringen här definieras som delta enlig QARTOD spike test.
        Ingen korrigering görs för hur långt ifrån varandra mätningarna ligger.
        Flaggning:
            GOOD_DATA: om förändringen ligger är < threshold low.
            BAD DATA CORRECTABLE: om threshold high > förändringen > threshold low.
            BAD_DATA: om förändringen är > threshold high-
        """
        self._parameter = parameter
        parameter_boolean = pl.col("parameter") == parameter

        # Early exit if nothing matches
        if self._data.filter(parameter_boolean).is_empty():
            return
        self._threshold_high = configuration.threshold_high
        selection = self._data.filter(
            parameter_boolean
            & pl.col("value").is_not_null()
            & (pl.col("TOTAL_QC") != "4")
        )
        selection = (
            selection.sort(["visit_key", "DEPH"])
            .with_columns(
                [
                    pl.col("value").shift(1).over("visit_key").alias("prev_value"),
                    pl.col("value").shift(-1).over("visit_key").alias("next_value"),
                    pl.col("value").shift(2).over("visit_key").alias("prev2_value"),
                    pl.col("value").shift(-2).over("visit_key").alias("next2_value"),
                    pl.col("DEPH").shift(1).over("visit_key").alias("prev_deph"),
                    pl.col("DEPH").shift(-1).over("visit_key").alias("next_deph"),
                    pl.col("DEPH").shift(2).over("visit_key").alias("prev2_deph"),
                    pl.col("DEPH").shift(-2).over("visit_key").alias("next2_deph"),
                ]
            )
            .with_columns(
                [
                    abs(pl.col("next_deph") - pl.col("prev_deph")).alias("deph_diff"),
                ]
            )
            .with_columns(
                [
                    (abs(pl.col("next_value") + pl.col("prev_value")) * 0.5).alias(
                        "spike_ref"
                    ),
                    (
                        pl.col("next_value")
                        * (
                            abs(pl.col("DEPH") - pl.col("next_deph"))
                            / pl.col("deph_diff")
                        )
                        + pl.col("prev_value")
                        * (
                            abs(pl.col("DEPH") - pl.col("prev_deph"))
                            / pl.col("deph_diff")
                        )
                    ).alias("weighted_spike_ref"),
                ]
            )
            .with_columns(
                [
                    abs(
                        abs(pl.col("value") - pl.col("weighted_spike_ref"))
                        - (abs(pl.col("next_value") - pl.col("prev_value")) * 0.5)
                    ).alias("delta"),
                    (
                        abs(pl.col("next_value") - pl.col("prev_value"))
                        / abs(pl.col("next_deph") - pl.col("prev_deph"))
                    ).alias("rate_of_change"),
                    (
                        abs(pl.col("next2_value") - pl.col("prev2_value"))
                        / abs(pl.col("next2_deph") - pl.col("prev2_deph"))
                    ).alias("long_rate_of_change"),
                ]
            )
        )

        result_expr = self._apply_flagging_logic(configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(self, configuration: SpikeCheck) -> pl.DataFrame:
        """
        Apply flagging logic for delta (spike) check.
        """
        """ The difference between sequential measurements, where one measurement is
        significantly different from adjacent ones, is a spike in both size and gradient.
        This test does not consider differences in depth, but assumes a sampling that
        adequately reproduces changes in the tested parameter"""

        result_expr = (
            pl.when(
                (
                    (pl.col("delta") < configuration.threshold_high)
                    & (pl.col("delta") >= configuration.threshold_low)
                    & (pl.col("rate_of_change") <= configuration.rate_of_change)
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA_CORRECTABLE.value)).alias("flag"),
                        pl.format(
                            "BAD DATA CORRECTABLE: threshold_high > spike >= threshold_low. {} > {} >= {}. Previous {}, Next {}, rate_of_change {}",  # noqa: E501
                            configuration.threshold_high,
                            pl.col("delta").round(2),
                            configuration.threshold_low,
                            pl.col("prev_value").round(2),
                            pl.col("next_value").round(2),
                            pl.col("rate_of_change").round(2),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (pl.col("delta") >= configuration.threshold_high)
                & (pl.col("rate_of_change") <= configuration.rate_of_change)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.format(
                            "BAD DATA: spike >= threshold_high. {} >= {}. Previous {}, Next {}, rate_of_change {}",  # noqa: E501
                            pl.col("delta").round(2),
                            configuration.threshold_high,
                            pl.col("prev_value").round(2),
                            pl.col("next_value").round(2),
                            pl.col("rate_of_change").round(2),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD DATA: spike < threshold_low. {} < {}. Rate of change {}.Tested values: {}, {}, {}",  # noqa: E501,
                            pl.col("delta").round(2),
                            configuration.threshold_low,
                            pl.col("rate_of_change").round(2),
                            pl.col("prev_value").round(2),
                            pl.col("value").round(2),
                            pl.col("next_value").round(2),
                        ).alias("info"),
                    ]
                )
            )
        )

        return result_expr
