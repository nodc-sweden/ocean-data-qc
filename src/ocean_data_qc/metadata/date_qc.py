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

        missing_sdate = 0
        missing_stime = 0
        bad_time_formats = []
        bad_date_formats = []
        date_in_future = []
        date_to_old = []

        for date_string, time_string in self._visit.times():
            if not date_string:
                missing_sdate += 1
                bad_date |= True
            if not time_string:
                missing_stime += 1
                bad_date |= True

            # Test time format
            try:
                time = datetime.datetime.strptime(time_string, "%H:%M").time()
            except (ValueError, TypeError):
                bad_date |= True
                if time_string:
                    bad_time_formats.append(time_string)
                time = None

            # Test date format
            try:
                date = datetime.datetime.strptime(date_string, "%Y-%m-%d")
            except (ValueError, TypeError):
                bad_date |= True
                if date_string:
                    bad_date_formats.append(date_string)
                date = None

            if None not in (time, date):
                date = datetime.datetime.combine(date, time)

            # Test date range
            if date and not first_date <= date <= now:
                if date > now:
                    date_in_future.append(date)
                else:
                    date_to_old.append(date)
                bad_date = True
        if bad_date:
            self._visit.qc[MetadataQcField.DateAndTime] = MetadataFlag.BAD_DATA

            if missing_sdate:
                self._visit.log(
                    MetadataQcField.DateAndTime,
                    "SDATE",
                    f"Missing in {missing_sdate} samples.",
                )
            if missing_stime:
                self._visit.log(
                    MetadataQcField.DateAndTime,
                    "STIME",
                    f"Missing in {missing_stime} samples.",
                )
            if bad_date_formats:
                self._visit.log(
                    MetadataQcField.DateAndTime,
                    "SDATE",
                    "Bad date format: " + ", ".join(map(str, bad_date_formats)),
                )
            if bad_time_formats:
                self._visit.log(
                    MetadataQcField.DateAndTime,
                    "STIME",
                    "Bad time format: " + ", ".join(map(str, bad_time_formats)),
                )
            if date_in_future:
                self._visit.log(
                    MetadataQcField.DateAndTime,
                    "STIME",
                    "Date in future: " + ", ".join(map(str, date_in_future)),
                )
                self._visit.log(
                    MetadataQcField.DateAndTime,
                    "SDATE",
                    "Date in future: " + ", ".join(map(str, date_in_future)),
                )
            if date_to_old:
                self._visit.log(
                    MetadataQcField.DateAndTime,
                    "STIME",
                    "Date too old: " + ", ".join(map(str, date_to_old)),
                )
                self._visit.log(
                    MetadataQcField.DateAndTime,
                    "SDATE",
                    "Date too old: " + ", ".join(map(str, date_to_old)),
                )
        else:
            self._visit.qc[MetadataQcField.DateAndTime] = MetadataFlag.GOOD_DATA
