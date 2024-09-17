import pytest
from freezegun import freeze_time
from ocean_data_qc.metadata.date_qc import DateQc
from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField
from ocean_data_qc.metadata.visit import Visit

from tests.setup_methods import generate_data_frame_from_data_list


@pytest.mark.parametrize(
    "given_missing_parameters, expected_flag",
    (
        (("SDATE",), MetadataFlag.BAD_DATA),
        (("STIME",), MetadataFlag.BAD_DATA),
        (("SDATE", "STIME"), MetadataFlag.BAD_DATA),
    ),
)
def test_both_date_and_time_are_required(given_missing_parameters, expected_flag):
    # Given data where some parameters can be missing
    given_data = {"SDATE": "2000-01-01", "STIME": "12:00"}
    for missing_parameter in given_missing_parameters:
        del given_data[missing_parameter]
    dataframe = generate_data_frame_from_data_list([given_data])

    # Given no qc has been made for visit
    visit = Visit(dataframe)
    assert visit.qc[MetadataQcField.DateAndTime] == MetadataFlag.NO_QC_PERFORMED

    # When performing QC
    date_qc = DateQc(visit)
    date_qc.check()

    # Then the parameter is given the expected flag at the expected position
    assert visit.qc[MetadataQcField.DateAndTime] == expected_flag

    # And the missing parameters are in the qc log
    assert set(visit.qc_log[MetadataQcField.DateAndTime].keys()) == set(
        given_missing_parameters
    )


@pytest.mark.parametrize(
    "given_current_date, given_date, expected_flag",
    (
        ("2024-09-07", "2024-09-06", MetadataFlag.GOOD_DATA),  # YYYY-MM-DD
        ("2024-09-07", "Today", MetadataFlag.BAD_DATA),  # Literal date
        ("2024-09-07", "240906", MetadataFlag.BAD_DATA),  # YYMMDD
        ("2024-09-07", "24-09-06", MetadataFlag.BAD_DATA),  # YY-MM-DD
        ("2024-09-07", "20240906", MetadataFlag.BAD_DATA),  # YYYYMMDD
        ("2024-09-07", "2024.09.06", MetadataFlag.BAD_DATA),  # YYYY.MM.DD
        ("2024-09-07", "6/9/24", MetadataFlag.BAD_DATA),  # M/D/YY
        ("2024-09-07", "09-06-2024", MetadataFlag.BAD_DATA),  # MM-DD-YYYY
        ("2024-09-07", "09/06/2024", MetadataFlag.BAD_DATA),  # MM/DD/YYYY
        ("2024-09-07", "06-09-2024", MetadataFlag.BAD_DATA),  # DD-MM-YYYY
        ("2024-09-07", "06.09.2024", MetadataFlag.BAD_DATA),  # DD.MM.YYYY
        ("2024-09-07", "2023-13-01", MetadataFlag.BAD_DATA),  # That's not a month
        ("2024-09-07", "2023-08-32", MetadataFlag.BAD_DATA),  # That's not a day
    ),
)
def test_dates_check_for_various_date_formats(
    given_current_date, given_date, expected_flag: MetadataFlag
):
    # Given data with a given date and a time
    given_data = generate_data_frame_from_data_list(
        [{"SDATE": given_date, "STIME": "12:00"}]
    )

    # Given no qc has been made for visit
    visit = Visit(given_data)
    assert visit.qc[MetadataQcField.DateAndTime] == MetadataFlag.NO_QC_PERFORMED

    # Given a current date
    with freeze_time(given_current_date):
        # When performing QC
        date_qc = DateQc(visit)
        date_qc.check()

    # Then the parameter is given the expected flag at the expected position
    assert visit.qc[MetadataQcField.DateAndTime] == expected_flag

    if expected_flag == MetadataFlag.GOOD_DATA:
        # And if good data is expected, the qc log is empty
        assert not visit.qc_log
    else:
        # And if bad data is expected, the value is added to the qc log
        assert "SDATE" in visit.qc_log[MetadataQcField.DateAndTime]
        assert any(
            given_date in entry
            for entry in visit.qc_log[MetadataQcField.DateAndTime]["SDATE"]
        )


@pytest.mark.parametrize(
    "given_time, expected_flag",
    (
        ("14:35", MetadataFlag.GOOD_DATA),  # HH:MM
        ("2:35 pm", MetadataFlag.BAD_DATA),  # HH:MM am/pm
        ("14.35", MetadataFlag.BAD_DATA),  # HH.MM
        ("1435", MetadataFlag.BAD_DATA),  # HHMM
        ("24:01", MetadataFlag.BAD_DATA),  # That's not a time
    ),
)
def test_date_check_for_various_time_formats(given_time, expected_flag: MetadataFlag):
    # Given data with a date and a given time
    given_data = generate_data_frame_from_data_list(
        [{"SDATE": "2000-01-01", "STIME": given_time}]
    )

    # Given no qc has been made for visit
    visit = Visit(given_data)
    assert visit.qc[MetadataQcField.DateAndTime] == MetadataFlag.NO_QC_PERFORMED

    # When performing QC
    date_qc = DateQc(visit)
    date_qc.check()

    # Then the parameter is given the expected flag at the expected position
    assert visit.qc[MetadataQcField.DateAndTime] == expected_flag

    if expected_flag == MetadataFlag.GOOD_DATA:
        # And if good data is expected, the qc log is empty
        assert not visit.qc_log
    else:
        # And if bad data is expected, the value is added to the qc log
        assert "STIME" in visit.qc_log[MetadataQcField.DateAndTime]
        assert any(
            given_time in entry
            for entry in visit.qc_log[MetadataQcField.DateAndTime]["STIME"]
        )


@pytest.mark.parametrize(
    "given_current_datetime, given_date, given_time, expected_flag",
    (
        (
            "2024-09-09T14:58:00",
            "2024-09-09",
            "14:58",
            MetadataFlag.GOOD_DATA,
        ),  # In the past
        (
            "2024-09-09T14:58:00",
            "2024-09-09",
            "14:59",
            MetadataFlag.BAD_DATA,
        ),  # In the future
        (
            "2024-09-09T14:58:00",
            "1892-12-31",
            "23:59",
            MetadataFlag.BAD_DATA,
        ),  # Before first possible date
        (
            "2024-09-09T14:58:00",
            "1893-01-01",
            "00:00",
            MetadataFlag.GOOD_DATA,
        ),  # On first possible date
    ),
)
def test_combined_check_of_date_and_time(
    given_current_datetime, given_date, given_time, expected_flag
):
    # Given data with a given date and time
    given_data = generate_data_frame_from_data_list(
        [{"SDATE": given_date, "STIME": given_time}]
    )

    # Given no qc has been made for visit
    visit = Visit(given_data)
    assert visit.qc[MetadataQcField.DateAndTime] == MetadataFlag.NO_QC_PERFORMED

    # Given a current datetime
    with freeze_time(given_current_datetime):
        # When performing QC
        date_qc = DateQc(visit)
        date_qc.check()

        # Then the parameter is given the expected flag at the expected position
        assert visit.qc[MetadataQcField.DateAndTime] == expected_flag

    if expected_flag == MetadataFlag.GOOD_DATA:
        # And if good data is expected, the qc log is empty
        assert not visit.qc_log
    else:
        # And if bad data is expected,
        # the value is added to thed qc log for both SDATE and STIME
        assert "SDATE" in visit.qc_log[MetadataQcField.DateAndTime]
        assert "STIME" in visit.qc_log[MetadataQcField.DateAndTime]
