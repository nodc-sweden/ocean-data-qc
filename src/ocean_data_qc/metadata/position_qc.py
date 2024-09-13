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
            if self.SWEREF99TM_PATTERN.fullmatch(
                latitude
            ) and self.SWEREF99TM_PATTERN.fullmatch(longitude):
                latitude, longitude = self.sweref99tm_to_wgs84(latitude, longitude)
            elif not (
                self.WGS84_DDDMM_SSS_PATTERN.fullmatch(latitude)
                and self.WGS84_DDDMM_SSS_PATTERN.fullmatch(longitude)
            ):
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

        if bad_position:
            self._visit.qc[MetadataQcField.Position] = MetadataFlag.BAD_DATA
            self._visit.log(
                MetadataQcField.Position,
                ("LATIT", "LONGI"),
                f"Position outside rough area: {latitude}, {longitude}",
            )
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
