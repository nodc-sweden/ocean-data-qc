import abc

from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flags import QcFlags


class BaseQcCategory(abc.ABC):
    def __init__(self, data, field_position: int, column_name: str):
        self._data = data
        self._field_position = field_position
        self._column_name = column_name

    @abc.abstractmethod
    def check(self, parameter: str, configuration): ...

    def expand_qc_columns(self):
        # Add minimal quality flags if missing
        if "quality_flag_long" not in self._data.columns:
            self._data["quality_flag_long"] = str(QcFlags())

        # Split QC flags to separate columns for incoming, auto and manual
        if not self._data.empty:
            self._data[["INCOMING_QC", "AUTO_QC", "MANUAL_QC", "TOTAL_QC"]] = self._data[
                "quality_flag_long"
            ].str.split("_", expand=True)

        # Add a column for the specific category
        self._data[self._column_name] = str(QcFlag.NO_QC_PERFORMED.value)

    def collapse_qc_columns(self):
        # Insert the specific QC flag to its correct position in the full auto QC string
        self._data["AUTO_QC"] = (
            self._data["AUTO_QC"].str[: self._field_position]
            + self._data[self._column_name]
            + self._data["AUTO_QC"].str[self._field_position + 1 :]
        )
        self._data.drop(self._column_name, inplace=True, axis=1)

        # Recreate the combined QC flags string from the parts (incoming, auto, manual)
        self._data["quality_flag_long"] = (
            self._data["INCOMING_QC"]
            + "_"
            + self._data["AUTO_QC"]
            + "_"
            + self._data["MANUAL_QC"]
            + "_"
            + self._data["TOTAL_QC"]
        )
