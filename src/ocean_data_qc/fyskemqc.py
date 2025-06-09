import importlib

import pandas as pd

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_configuration import QcConfiguration
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from ocean_data_qc.fyskem.qc_flags import QcFlags


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
        for qc_category in QcField:
            print(f"run {qc_category} qc")
            # Get config for parameter
            class_name = f"{qc_category.name}Qc"
            module_name = f"ocean_data_qc.fyskem.{qc_category.name.lower()}_qc"
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            category_checker = cls(self._data)
            category_checker.expand_qc_columns()

            for parameter in self._configuration.parameters(
                f"{qc_category.name.lower()}_check"
            ):
                if config := self._configuration.get(
                    f"{qc_category.name.lower()}_check", parameter
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
