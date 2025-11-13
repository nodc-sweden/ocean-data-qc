import numpy as np
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
        selection = (
            self._data.filter(
                parameter_boolean
                & pl.col("value").is_not_null()
                & (pl.col("TOTAL_QC") != "4")
            )
            .sort(["visit_key", "DEPH"])
            .with_columns(
                [
                    pl.col("value").shift(1).over("visit_key").alias("prev_value"),
                    pl.col("value").shift(-1).over("visit_key").alias("next_value"),
                    pl.col("DEPH").shift(1).over("visit_key").alias("prev_deph"),
                    pl.col("DEPH").shift(-1).over("visit_key").alias("next_deph"),
                ]
            )
            .with_columns(
                [
                    (abs(pl.col("next_value") + pl.col("prev_value")) * 0.5).alias(
                        "spike_ref"
                    ),
                ]
            )
            .with_columns(
                [
                    (
                        abs(pl.col("value") - pl.col("spike_ref"))
                        - (abs(pl.col("next_value") - pl.col("prev_value")) * 0.5)
                    )
                    .abs()
                    .alias("delta"),
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
        result_expr = (
            pl.when(pl.col("delta").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.NO_QC_PERFORMED.value)).alias("flag"),
                        pl.format(
                            "MISSING no value for delta {}",
                            pl.col("delta"),
                        ).alias("info"),
                    ]
                )
            )
            .when((pl.col("delta") >= configuration.threshold_high))
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.format(
                            "BAD DATA: value - (spike_ref - halva diff) >= threshold_high. {} >= {}. Previous {}, Next {}",  # noqa: E501
                            pl.col("delta").round(2),
                            configuration.threshold_high,
                            pl.col("prev_value").round(2),
                            pl.col("next_value").round(2),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (pl.col("delta") < configuration.threshold_high)
                & (pl.col("delta") >= configuration.threshold_low)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA_CORRECTABLE.value)).alias("flag"),
                        pl.format(
                            "BAD DATA CORRECTABLE: value - (spike_ref - halva diff) >= threshold_low. {} > {} >= {}. Previous {}, Next {}",  # noqa: E501
                            configuration.threshold_high,
                            pl.col("delta").round(2),
                            configuration.threshold_low,
                            pl.col("prev_value").round(2),
                            pl.col("next_value").round(2),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD: value - (spike_ref - halva diff) < threshold_low. {}-{} < {}. Previous {}, Next {}",  # noqa: E501
                            pl.col("value"),
                            pl.col("delta").round(2),
                            configuration.threshold_low,
                            pl.col("prev_value").round(2),
                            pl.col("next_value").round(2),
                        ).alias("info"),
                    ]
                )
            )
        )

        return result_expr

    def _calculate_deltas(self, profile):
        """

        Perform spike detection on a single profile sorted by depth.
        The test is designed according to ARGO recommendations for profiling float
        (Thierry, Bittig et al 2018), https://archimer.ifremer.fr/doc/00354/46542/82301.pdf
        The difference between sequential measurements, where one measurement is
        significantly different from adjacent ones, is a spike in both size and gradient.
        This test does not consider differences in depth, but assumes a sampling
        that adequately reproduces changes in DOXY and TEMP_DOXY with depth.
        Test value = | V2 - (V3 + V1)/2 | - | (V3 - V1) / 2 |
        where V2 is the measurement being tested as a spike,
        and V1 and V3 are the values above andbelow.
        For DOXY: The V2 value is flagged when
        - the test value exceeds 50 micromol/kg for pressures < 500 dbar, or
        - the test value exceeds 25 micromol/kg for pressures >= to 500 dbar.

        """
        profile = profile.sort_values(by="DEPH").reset_index(drop=True)
        vals = profile["value"].values

        deltas = np.full(len(profile), np.nan, dtype=float)
        if len(vals) > 2:
            prev_value = vals[:-2]
            next_value = vals[2:]
            alfa = vals[1:-1] - np.abs((prev_value + next_value) / 2)
            gradient = np.abs((next_value - prev_value) / 2)
            delta = np.round(np.abs(alfa) - np.abs(gradient), 2)
            deltas[1:-1] = delta

        profile["delta"] = deltas
        return profile


def delta(v):
    vals = np.array(v)
    prev_value = vals[:-2]
    next_value = vals[2:]
    print(next_value)
    print(prev_value)
    (next_value - prev_value)
    np.abs((prev_value + next_value) / 2)
    alfa = vals[1:-1] - np.abs((prev_value + next_value) / 2)
    gradient = np.abs((next_value - prev_value) / 2)
    delta = np.round(np.abs(alfa) - np.abs(gradient), 2)

    return delta


if __name__ == "__main__":
    d = [
        1,
        5,
        10,
        15,
        20,
        25,
        30,
        40,
        50,
        60,
    ]
    v = [
        20.3,
        19.3,
        18,
        17.6,
        17.1,
        15.9,
        13.8,
        15,
        14.9,
        15.1,
    ]
    deltas = delta(v)
    print([d[1:-1], v[1:-1], deltas])
