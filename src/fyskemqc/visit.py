import pandas as pd

from fyskemqc.errors import VisitError
from fyskemqc.metadata_qc.metadata_flag import MetadataFlag
from fyskemqc.metadata_qc.metadata_qc_field import MetadataQcField


class Visit:
    METADATA_FIELDS = ("WADEP",)

    def __init__(self, data: pd.DataFrame):
        self._data = data

        if len(series_ids := self._data.SERNO.unique()) > 1:
            formated_ids = "', '".join(series_ids)
            raise VisitError(f"Visit data contains multiple series ids: '{formated_ids}'")
        self._series = series_ids[0]

        if len(station_names := self._data.STATN.unique()) > 1:
            formated_stations = "', '".join(station_names)
            raise VisitError(
                f"Visit data contains multiple stations: '{formated_stations}'"
            )
        self._station = station_names[0]

        self._qc_fields = {
            field: MetadataFlag.NO_QC_PERFORMED for field in MetadataQcField
        }

        self._init_metadata()

    def _init_metadata(self):
        self._metadata = {}
        for field in self.METADATA_FIELDS:
            if field not in self._data.columns:
                continue

            if len(values := self._data[field].unique()) > 1:
                print("Gör något")
            else:
                self._metadata[field] = values[0]

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

    @property
    def metadata(self):
        return self._metadata
