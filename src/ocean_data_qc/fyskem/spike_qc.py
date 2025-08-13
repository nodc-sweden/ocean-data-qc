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
            self._data.filter(parameter_boolean)
            .sort(["visit_key", "DEPH"])
            .with_columns(
                [
                    pl.col("value").shift(1).over("visit_key").alias("v_minus"),
                    pl.col("value").shift(-1).over("visit_key").alias("v_plus"),
                ]
            )
            .with_columns(
                [
                    (pl.col("value") - ((pl.col("v_minus") + pl.col("v_plus")) / 2).abs())
                    .abs()
                    .alias("alfa"),
                    ((pl.col("v_plus") - pl.col("v_minus")) / 2).abs().alias("gradient"),
                ]
            )
            .with_columns(
                [(pl.col("alfa") - pl.col("gradient")).abs().round(2).alias("delta")]
            )
            .drop(["v_minus", "v_plus", "alfa", "gradient"])  # optional cleanup
        )

        self._apply_flagging_logic(selection, configuration)

    def _apply_flagging_logic(
        self, selection: pl.DataFrame, configuration: SpikeCheck
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
            .when(pl.col("delta").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.NO_QC_PERFORMED.value)).alias("flag"),
                        pl.format(
                            "NO_QC_PERFORMED delta missing for {}",
                            pl.lit(self._parameter),
                        ).alias("info"),
                    ]
                )
            )
            .when(pl.col("delta") >= self._threshold)
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA_CORRECTABLE.value)).alias("flag"),
                        pl.format(
                            "CORRECTABLE spike detected, {} exceeds allowed delta {}",
                            pl.col("delta"),
                            pl.lit(self._threshold),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD delta {} within allowed delta {}",
                            pl.col("delta"),
                            pl.lit(self._threshold),
                        ).alias("info"),
                    ]
                )
            )
        )

        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

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
            v_minus = vals[:-2]
            v_plus = vals[2:]
            alfa = vals[1:-1] - np.abs((v_minus + v_plus) / 2)
            gradient = np.abs((v_plus - v_minus) / 2)
            delta = np.round(np.abs(alfa) - np.abs(gradient), 2)
            deltas[1:-1] = delta

        profile["delta"] = deltas
        return profile


def delta(v):
    vals = np.array(v)
    v_minus = vals[:-2]
    v_plus = vals[2:]
    print(v_plus)
    print(v_minus)
    (v_plus - v_minus)
    np.abs((v_minus + v_plus) / 2)
    alfa = vals[1:-1] - np.abs((v_minus + v_plus) / 2)
    gradient = np.abs((v_plus - v_minus) / 2)
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
