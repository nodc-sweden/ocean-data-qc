import numpy as np
import pandas as pd
import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import SpikeCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class SpikeQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.SpikeCheck, f"AUTO_QC_{QcField.SpikeCheck.name}")

    def check(self, parameter: str, configuration: SpikeCheck):
        """
        check som kollar förändring mellan föregående djup och nästa djup
        GOOD_DATA: om förändringen ligger mellan allowed_increase och allowed_decrease
        BAD_DATA: om värdet på parameter utanför intervallet
        """
        self._threshold = configuration.allowed_delta
        self._parameter = parameter

        parameter_boolean = self._data.parameter == parameter
        selection = self._data.loc[parameter_boolean]
        # First value (normally surface) will always be nan.
        selection = selection.groupby("visit_key", group_keys=False)[
            ["value", "DEPH"]
        ].apply(self._calculate_deltas)
        if selection.empty:
            return

        selection = self._apply_polars_flagging_logic(selection, configuration)
        self._data.loc[parameter_boolean, [self._column_name, self._info_column_name]] = (
            selection[[self._column_name, self._info_column_name]].values
        )

    def _apply_polars_flagging_logic(
        self, selection: pd.DataFrame, configuration: SpikeCheck
    ) -> pd.DataFrame:
        """
        Apply flagging logic for delta (spike) check.
        """
        pl_selection = pl.from_pandas(selection)

        result_expr = (
            pl.when(pl.col("value").is_null())
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

        pl_selection = (
            pl_selection.with_columns([result_expr.alias("result_struct")])
            .with_columns(
                [
                    pl.col("result_struct").struct.field("flag").alias(self._column_name),
                    pl.col("result_struct")
                    .struct.field("info")
                    .alias(self._info_column_name),
                ]
            )
            .drop("result_struct")
        )

        return pl_selection.to_pandas()

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


# def delta(v, d):
#     f = np.abs((d[1]-d[0])/(d[2]-d[0]))
#     a = v[1] - np.abs((v[0]+v[2])*f)
#     b = np.abs((v[2]-v[0])*f)
#     d = np.abs(a-b)
#     return f, round(a, 2), round(b,2), round(d,2)
