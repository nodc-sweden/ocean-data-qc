from typing import Union

import polars as pl

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
        "LATIT_NOM",
        "LONGI",
        "LONGI_NOM",
        "SERNO",
        "SHIPC",
        "STATN",
        "WADEP",
        "WINDIR",
        "WINSP",
    )

    def __init__(self, data: pl.DataFrame):
        self._data = data
        self._series = (
            self._data.get_column("SERNO", default=pl.Series("SERNO", []))
            .unique()
            .to_list()
        )
        self._station = (
            self._data.get_column("STATN", default=pl.Series("STATN", []))
            .unique()
            .to_list()
        )
        self._times = set()
        self._positions = set()

        self._qc_fields = {
            field: MetadataFlag.NO_QC_PERFORMED for field in MetadataQcField
        }
        self._qc_log = {}

        self._init_metadata()

    def _init_metadata(self):
        self._metadata = {}

        if "SDATE" not in self._data.columns:
            self._data.insert_column(-1, pl.Series("SDATE", [""] * self._data.height))
        if "STIME" not in self._data.columns:
            self._data.insert_column(-1, pl.Series("STIME", [""] * self._data.height))

        self._times = set(self._data.select(["SDATE", "STIME"]).unique().iter_rows())

        if "LATIT" in self._data.columns and "LONGI" in self._data.columns:
            self._positions = set(
                self._data.select(["LATIT", "LONGI"]).unique().iter_rows()
            )
        elif "LATIT_NOM" in self._data.columns and "LONGI_NOM" in self._data.columns:
            self._positions = set(
                self._data.select(["LATIT_NOM", "LONGI_NOM"]).unique().iter_rows()
            )

        for field in self.METADATA_FIELDS:
            if field not in self._data.columns:
                continue
            self._metadata[field] = self._data.get_column(field).unique().to_list()

    @property
    def series(self):
        return self._series

    @property
    def station(self):
        return self._station

    @property
    def qc(self):
        return self._qc_fields

    @property
    def qc_log(self):
        return self._qc_log

    def log(self, qc_field: MetadataQcField, parameters: Union[str, tuple], message: str):
        if isinstance(parameters, str):
            parameters = (parameters,)

        for parameter in parameters:
            if qc_field not in self._qc_log:
                self._qc_log[qc_field] = {parameter: []}
            if parameter not in self._qc_log[qc_field]:
                self._qc_log[qc_field][parameter] = []
            self._qc_log[qc_field][parameter].append(message)

    def water_depths(self):
        return self._data.get_column("DEPH").unique().to_list()

    def times(self):
        return self._times

    def positions(self):
        return self._positions

    @property
    def metadata(self):
        return self._metadata
