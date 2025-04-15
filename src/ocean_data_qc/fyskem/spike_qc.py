import numpy as np
import pandas as pd

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import SpikeCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class SpikeQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.SpikeCheck, f"AUTO_QC_{QcField.SpikeCheck}")

    def check(self, parameter: str, configuration: SpikeCheck):
        """
        check som kollar förändring mellan föregående djup och nästa djup
        GOOD_DATA: om förändringen ligger mellan allowed_increase och allowed_decrease
        BAD_DATA: om värdet på parameter utanför intervallet
        """
        self.threshold = configuration.allowed_delta
        boolean_selection = self._data.parameter == parameter

        selection = self._data.loc[boolean_selection]
        print(f"selection: {selection}")
        # First value (normally surface) will always be nan.
        selection = selection.groupby("visit_key", group_keys=False)[
            ["value", "DEPH"]
        ].apply(self._spike_check)
        print(f"selection after spike check: \n{selection=}")
        if selection.empty:
            return

        self._data.loc[boolean_selection, self._column_name] = np.where(
            pd.isna(selection["value"]),
            str(QcFlag.MISSING_VALUE.value),
            np.where(
                pd.isna(selection["delta"]),
                str(QcFlag.NO_QC_PERFORMED.value),
                np.where(
                    selection.delta >= configuration.allowed_delta,
                    str(QcFlag.BAD_DATA.value),
                    str(QcFlag.GOOD_DATA.value),
                ),
            ),
        )

    def _spike_check(self, profile):
        """

        Perform spike detection on a single profile sorted by depth.
        The test is designed according to ARGO recommendations for profiling float
        (Thierry, Bittig et al 2018), https://archimer.ifremer.fr/doc/00354/46542/82301.pdf
        The difference between sequential measurements, where one measurement is
        significantly different from adjacent ones, is a spike in both size and gradient.
        This test does not consider differences in depth, but assumes a sampling
        that adequately reproduces changes in DOXY and TEMP_DOXY with depth.
        Test value = | V2 − (V3 + V1)/2 | − | (V3 − V1) / 2 |
        where V2 is the measurement being tested as a spike,
        and V1 and V3 are the values above andbelow.
        For DOXY: The V2 value is flagged when
        - the test value exceeds 50 micromol/kg for pressures < 500 dbar, or
        - the test value exceeds 25 micromol/kg for pressures >= to 500 dbar.

        """
        profile = profile.sort_values(by="DEPH").reset_index(drop=True)
        vals = profile["value"].values
        dephs = profile["DEPH"].values

        deltas = np.full(len(profile), np.nan, dtype=float)

        if len(vals) > 2:
            v_minus = vals[:-2]
            v_plus = vals[2:]
            alfa = vals[1:-1] - np.abs((vals[:-2] + vals[2:]) / 2)
            gradient = np.abs((vals[2:] - vals[:-2]) / 2)
            delta = np.abs(alfa) - np.abs(gradient)
            deltas[1:-1] = delta
            print(delta)
            print("|Depth | v-1   | values | v+1 | Alfa  | gradient  | Delta|")
            print("|-" * 7)
            for d, a, b, g, v, v1, v3 in zip(
                dephs[1:-1], alfa, gradient, delta, vals[1:-1], v_minus, v_plus
            ):
                print(
                    f"|{d:^5.0f} | {v1:^5.2f} | {v:^5.2f} | {v3:^5.2f} | {a:^5.2f} | {b:^5.2f} | {g:^5.2f}|"  # noqa: E501
                )

        profile["delta"] = deltas
        return profile


# def delta(v, d):
#     f = np.abs((d[1]-d[0])/(d[2]-d[0]))
#     a = v[1] - np.abs((v[0]+v[2])*f)
#     b = np.abs((v[2]-v[0])*f)
#     d = np.abs(a-b)
#     return f, round(a, 2), round(b,2), round(d,2)
