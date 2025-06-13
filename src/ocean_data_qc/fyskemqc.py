import pandas as pd

from ocean_data_qc.fyskem.consistency_qc import ConsistencyQc
from ocean_data_qc.fyskem.detectionlimit_qc import DetectionLimitQc
from ocean_data_qc.fyskem.h2s_qc import H2sQc
from ocean_data_qc.fyskem.increasedecrease_qc import IncreaseDecreaseQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_configuration import QcConfiguration
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from ocean_data_qc.fyskem.qc_flags import QcFlags
from ocean_data_qc.fyskem.range_qc import RangeQc
from ocean_data_qc.fyskem.spike_qc import SpikeQc
from ocean_data_qc.fyskem.statistic_qc import StatisticQc

QC_CATEGORIES = (
    RangeQc,
    DetectionLimitQc,
    SpikeQc,
    StatisticQc,
    ConsistencyQc,
    H2sQc,
    IncreaseDecreaseQc,
)


class FysKemQc:
    def __init__(self, data: pd.DataFrame):
        self._data = data
        self._configuration = QcConfiguration()
        self._original_automatic_flags = self._data["quality_flag_long"].copy()

    def __len__(self):
        return len(self._data)

    def __getitem__(self, index):
        series = self._data.iloc[index]
        return Parameter(series)

    @property
    def parameters(self):
        return {Parameter(series) for _, series in self._data.iterrows()}

    def run_automatic_qc(self):
        ordered_qc_tests = sorted(
            (QcField[category.__name__.removesuffix("Qc")], category)
            for category in QC_CATEGORIES
        )
        for field, qc_category in ordered_qc_tests:
            print(f"run {field.name} qc")
            # Get config for parameter
            category_checker = qc_category(self._data)
            category_checker.expand_qc_columns()

            for parameter in self._configuration.parameters(
                f"{field.name.lower()}_check"
            ):
                if config := self._configuration.get(
                    f"{field.name.lower()}_check", parameter
                ):
                    category_checker.check(parameter, config)

            category_checker.collapse_qc_columns()

        self._update_total()

    def _update_total(self):
        changed_mask = self._data["quality_flag_long"] != self._original_automatic_flags

        if changed_mask.any():
            self._data.loc[changed_mask, "quality_flag_long"] = self._data.loc[
                changed_mask, "quality_flag_long"
            ].apply(lambda x: str(QcFlags.from_string(x)))
