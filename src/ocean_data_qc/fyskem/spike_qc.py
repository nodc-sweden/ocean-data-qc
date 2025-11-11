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
        check som kollar förändring mellan föregående djup och nästa djup
        GOOD_DATA: om förändringen ligger mellan allowed_increase och allowed_decrease
        BAD_DATA: om värdet på parameter utanför intervallet
        """
        self._parameter = parameter
        parameter_boolean = pl.col("parameter") == parameter

        # Early exit if nothing matches
        if self._data.filter(parameter_boolean).is_empty():
            return
        self._threshold = configuration.allowed_delta
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
                    (
                        (
                            (
                                (pl.col("next_value") - pl.col("prev_value"))
                                / (pl.col("next_deph") - pl.col("prev_deph"))
                            )  # rate of change pre m
                            * (
                                pl.col("DEPH") - pl.col("prev_deph")
                            )  # expected change from prev
                        )
                        + pl.col("prev_value")
                    ).alias("expected")  # slope * deph to get value if no spike
                ]
            )
            .with_columns(
                (pl.col("value") - pl.col("expected"))
                .abs()
                .alias("alfa")  # current value - expected from the slope at DEPH
            )
            # .drop(["prev_value", "next_value", "alfa", "gradient"])  # optional cleanup
        )

        result_expr = self._apply_flagging_logic(configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(self, configuration: SpikeCheck) -> pl.DataFrame:
        """
        Apply flagging logic for delta (spike) check.
        """
        result_expr = (
            pl.when(pl.col("alfa").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.NO_QC_PERFORMED.value)).alias("flag"),
                        pl.format(
                            "MISSING no value for expected {} or value {}",
                            pl.col("expected"),
                            pl.col("value"),
                        ).alias("info"),
                    ]
                )
            )
            .when(pl.col("alfa") >= self._threshold)
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA_CORRECTABLE.value)).alias("flag"),
                        pl.format(
                            "BAD DATA CORRECTABLE: value-expected >= {} ml/l. {}-{} = {}",
                            self._threshold,
                            pl.col("value"),
                            pl.col("expected").round(2),
                            pl.col("alfa").round(2),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD delta: value - expected < {} ml/l. {} - {} = {}",
                            self._threshold,
                            pl.col("value"),
                            pl.col("expected").round(2),
                            pl.col("alfa").round(2),
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
