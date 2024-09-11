import pandas as pd

from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField


class Visit:
    METADATA_FIELDS = (
        "AIRPRES",
        "AIRTEMP",
        "COMNT_VISIT",
        "CRUISE_NO",
        "CTRYID",
        "LATIT",
        "LONGI",
        "SHIPC",
        "STATN",
        "SERNO",
        "WADEP",
        "WINDIR",
        "WINSP",
    )

    def __init__(self, data: pd.DataFrame):
        self._data = data
        self._series = self._data.SERNO.unique() if "SERNO" in self._data.columns else []
        self._station = self._data.STATN.unique() if "STATN" in self._data.columns else []

        self._qc_fields = {
            field: MetadataFlag.NO_QC_PERFORMED for field in MetadataQcField
        }

        self._init_metadata()

    def _init_metadata(self):
        self._metadata = {}

        if "SDATE" not in self._data.columns:
            self._data["SDATE"] = ""
        if "STIME" not in self._data.columns:
            self._data["STIME"] = ""

        self._times = set(
            tuple(self._data[["SDATE", "STIME"]].itertuples(index=False, name=None))
        )
        for field in self.METADATA_FIELDS:
            if field not in self._data.columns:
                continue
            self._metadata[field] = self._data[field].unique()

    @property
    def series(self):
        return self._series

    @property
    def station(self):
        return self._station

    @property
    def qc(self):
        return self._qc_fields

    def water_depths(self):
        return self._data.DEPH.unique()

    def times(self):
        return self._times

    @property
    def metadata(self):
        return self._metadata
