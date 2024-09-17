import math
import re

import pyproj

from ocean_data_qc.metadata.base_metadata_qc_category import BaseMetadataQcCategory
from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField


class PositionQc(BaseMetadataQcCategory):
    SWEREF99TM_PATTERN = re.compile(r"\d{6,7}(?:\.\d{1,3})?")
    WGS84_DDDMM_SSS_PATTERN = re.compile(r"\d{3,5}(?:\.\d{1,3})?")
    southern_limit = 5348
    northen_limit = 6600
    western_limit = 400
    eastern_limit = 3100

    def check(self):
        bad_position = False
        for latitude, longitude in self._visit.positions():
            if self._is_sweref99tm(latitude, longitude):
                latitude, longitude = self.sweref99tm_to_wgs84(latitude, longitude)
            elif not self._is_wgs84(latitude, longitude):
                bad_position |= True
                self._visit.log(
                    MetadataQcField.Position,
                    ("LATIT", "LONGI"),
                    f"Bad position format: {latitude}, {longitude}",
                )
                continue
            else:
                latitude = float(latitude)
                longitude = float(longitude)

            if not (
                self.southern_limit <= latitude <= self.northen_limit
                and self.western_limit <= longitude <= self.eastern_limit
            ):
                bad_position |= True
                self._visit.log(
                    MetadataQcField.Position,
                    ("LATIT", "LONGI"),
                    f"Position outside rough area: {latitude}, {longitude}",
                )

        if bad_position:
            self._visit.qc[MetadataQcField.Position] = MetadataFlag.BAD_DATA
        else:
            self._visit.qc[MetadataQcField.Position] = MetadataFlag.GOOD_DATA

    @staticmethod
    def sweref99tm_to_wgs84(sweref99_latitude, sweref99_longitude):
        """Transform position from SWEREF99 TM to WGS84 DM"""
        transformer = pyproj.Transformer.from_crs(
            "EPSG:3006", "EPSG:4326", always_xy=True
        )
        wgs84_longitude, wgs84_latitude = transformer.transform(
            yy=sweref99_latitude,
            xx=sweref99_longitude,
        )

        return PositionQc._dd_to_dm(wgs84_latitude), PositionQc._dd_to_dm(wgs84_longitude)

    @staticmethod
    def _dd_to_dm(dd_value):
        remainder, degrees = math.modf(dd_value)
        return degrees * 100 + remainder * 60

    @classmethod
    def _is_sweref99tm(cls, latitude, longitude):
        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except ValueError:
            return False

        if not 181896.33 <= longitude <= 1086312.94:
            return False

        if not 6090353.78 <= latitude <= 7689478.31:
            return False

        return True

    @classmethod
    def _is_wgs84(cls, latitude, longitude):
        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except ValueError:
            return False

        latitude_degrees, latitude_hours = cls._split_ddhh(latitude)
        if latitude_degrees > 90:
            return False

        if latitude_hours >= 60:
            return False

        longitude_degrees, longitude_hours = cls._split_ddhh(longitude)
        if longitude_degrees > 180:
            return False

        if longitude_hours >= 60:
            return False

        return True

    @classmethod
    def _split_ddhh(cls, latitude):
        degrees_hours_latitude = f"{abs(int(latitude)):04}"
        latitude_degrees, latitude_hours = (
            int(degrees_hours_latitude[:2]),
            int(degrees_hours_latitude[2:]),
        )
        return latitude_degrees, latitude_hours
