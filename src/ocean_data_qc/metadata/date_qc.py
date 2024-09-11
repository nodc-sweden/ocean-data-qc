import datetime

from ocean_data_qc.metadata.base_metadata_qc_category import BaseMetadataQcCategory
from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField


class DateQc(BaseMetadataQcCategory):
    def check(self):
        bad_date = False
        first_date = datetime.datetime(
            1893,
            1,
            1,
        )
        now = datetime.datetime.now()

        for date_string, time_string in self._visit.times():
            # Test format
            try:
                time = datetime.datetime.strptime(time_string, "%H:%M").time()
            except (ValueError, TypeError):
                bad_date |= True
                time = None

            # Test format
            try:
                date = datetime.datetime.strptime(date_string, "%Y-%m-%d")
            except (ValueError, TypeError):
                bad_date |= True
                date = None

            if None not in (time, date):
                date = datetime.datetime.combine(date, time)

            # Test date range
            if date and not first_date <= date <= now:
                bad_date = True
        if bad_date:
            self._visit.qc[MetadataQcField.DateAndTime] = MetadataFlag.BAD_DATA
        else:
            self._visit.qc[MetadataQcField.DateAndTime] = MetadataFlag.GOOD_DATA
