import pandas as pd

from ocean_data_qc.fyskem.consistency_qc import ConsistencyQc
from ocean_data_qc.fyskem.detection_limit_qc import DetectionLimitQc
from ocean_data_qc.fyskem.h2s_qc import H2sQc
from ocean_data_qc.fyskem.increasedecrease_qc import IncreaseDecreaseQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_configuration import QcConfiguration
from ocean_data_qc.fyskem.qc_flags import QcFlags
from ocean_data_qc.fyskem.range_qc import RangeQc
from ocean_data_qc.fyskem.spike_qc import SpikeQc
from ocean_data_qc.fyskem.statistic_qc import StatisticQc

QC_CATEGORIES = {
    "range_check": RangeQc,
    "detection_limit_check": DetectionLimitQc,
    "spike_check": SpikeQc,
    "statistic_check": StatisticQc,
    "consistency_check": ConsistencyQc,
    "h2s_check": H2sQc,
    "increasedecrease_check": IncreaseDecreaseQc,
}


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
        for category in QC_CATEGORIES.keys():
            # Get config for parameter
            category_checker = QC_CATEGORIES[category](self._data)
            category_checker.expand_qc_columns()

            for parameter in self._configuration.parameters(category):
                if config := self._configuration.get(category, parameter):
                    category_checker.check(parameter, config)

            category_checker.collapse_qc_columns()

        self._update_total()

    def _update_total(self):
        changed_mask = self._data["quality_flag_long"] != self._original_automatic_flags

        if changed_mask.any():
            self._data.loc[changed_mask, "quality_flag_long"] = self._data.loc[
                changed_mask, "quality_flag_long"
            ].apply(lambda x: str(QcFlags.from_string(x)))
