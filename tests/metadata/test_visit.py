import pytest
from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField
from ocean_data_qc.metadata.visit import Visit

from tests.setup_methods import generate_data_frame_from_data_list


@pytest.mark.parametrize(
    "given_series_id, given_station_name",
    (
        ("001", "Vinga"),
        ("999", "Sjösala"),
        ("042", "Sunnanö"),
    ),
)
def test_visit_wraps_pandas_series(given_series_id, given_station_name):
    # Given a dataframe with data for a specific visit
    given_data = generate_data_frame_from_data_list(
        [
            {"SERNO": given_series_id, "STATN": given_station_name},
            {"SERNO": given_series_id, "STATN": given_station_name},
            {"SERNO": given_series_id, "STATN": given_station_name},
        ]
    )

    # When creating a visit
    visit = Visit(given_data)

    # Then the given series id and station name can be retrieved
    assert visit.series == given_series_id
    assert visit.station == given_station_name


def test_visit_handles_conflicting_values_for_series_id():
    # Given three visit series numbers
    given_series_number_1 = "001"
    given_series_number_2 = "002"
    given_series_number_3 = "003"

    # Given a dataframe with data for the given visits  to a given station
    given_station = "Mariana trench"
    given_data = generate_data_frame_from_data_list(
        [
            {"SERNO": given_series_number_1, "STATN": given_station},
            {"SERNO": given_series_number_2, "STATN": given_station},
            {"SERNO": given_series_number_3, "STATN": given_station},
        ]
    )

    # When creating a visit
    visit = Visit(given_data)

    # Then visit stores all the values
    assert len(visit._series) == 3
    assert given_series_number_1 in visit._series
    assert given_series_number_2 in visit._series
    assert given_series_number_3 in visit._series


def test_visit_handles_conflicting_values_for_station_name():
    # Given three station names
    given_station_name_1 = "Yavin"
    given_station_name_2 = "Echo"
    given_station_name_3 = "Endor"

    # Given a dataframe with data for a visit with multiple stations
    given_series_id = "123"
    given_data = generate_data_frame_from_data_list(
        [
            {"SERNO": given_series_id, "STATN": given_station_name_1},
            {"SERNO": given_series_id, "STATN": given_station_name_2},
            {"SERNO": given_series_id, "STATN": given_station_name_3},
        ]
    )

    # When creating a visit
    visit = Visit(given_data)

    # Then visit stores all the values
    assert len(visit._station) == 3
    assert given_station_name_1 in visit._station
    assert given_station_name_2 in visit._station
    assert given_station_name_3 in visit._station


def test_visit_initializes_all_metadata_qc_flags_as_not_performed():
    given_data = generate_data_frame_from_data_list([{"SERNO": "123", "STATN": "ABC"}])

    visit = Visit(given_data)

    # Then all metadata is set to 'No QC performed'
    assert len(MetadataQcField)
    for category in MetadataQcField:
        assert visit.qc[category] == MetadataFlag.NO_QC_PERFORMED


def test_visit_handles_date_and_time_together():
    # Given a list of pairs of date and time including repeated values
    given_date_time_pairs = (
        ("2024-09-09", "23:58"),
        ("2024-09-09", "23:59"),
        ("2024-09-10", "0:00"),
        ("2024-09-10", "0:01"),
        ("2024-09-10", "0:01"),
    )
    # Given data with a spread in date and time
    given_data = generate_data_frame_from_data_list(
        [
            {"SERNO": "123", "STATN": "Station", "SDATE": date, "STIME": time}
            for date, time in given_date_time_pairs
        ]
    )

    # When creating a visit
    visit = Visit(given_data)

    # Then all unique date, time pairs are kept together
    assert len(visit.times()) < len(given_date_time_pairs)
    assert visit.times() == set(given_date_time_pairs)


def test_visit_handles_latitude_and_longitude_together():
    # Given a list of pairs of date and time including repeated values
    given_position_pairs = (
        ("5606.984", "1631.162"),
        ("5606.985", "1631.162"),
        ("5606.985", "1631.163"),
        ("5606.986", "1631.163"),
        ("5606.986", "1631.163"),
    )

    # Given data with a spread in date and time
    given_data = generate_data_frame_from_data_list(
        [
            {"SERNO": "123", "STATN": "Station", "LATIT": latitude, "LONGI": longitude}
            for latitude, longitude in given_position_pairs
        ]
    )

    # When creating a visit
    visit = Visit(given_data)

    # Then all unique date, time pairs are kept together
    assert len(visit.positions()) < len(given_position_pairs)
    assert visit.positions() == set(given_position_pairs)


def test_visit_uses_nominal_position_when_measured_position_is_missing():
    # Given nominal positions
    given_nominal_positions = (
        ("5606", "1631"),
        ("5605", "1630"),
    )
    # Given data with LONGI_NOM and LATIIT_NOM but not LATIT and LONGO
    given_data = generate_data_frame_from_data_list(
        [
            {
                "SERNO": "123",
                "STATN": "Station",
                "LATIT_NOM": latitude,
                "LONGI_NOM": longitude,
            }
            for latitude, longitude in given_nominal_positions
        ]
    )

    # When creating a visit
    visit = Visit(given_data)

    # Then positions will be taken from the nominal values
    assert visit.positions() == set(given_nominal_positions)


def test_visit_uses_measured_position_over_nominal_position_when_both_are_available():
    # Given measured positions
    given_measured_positions = (
        ("5606.984", "1631.162"),
        ("5606.985", "1631.163"),
        ("5606.986", "1631.164"),
    )

    # Given nominal positions
    given_nominal_positions = (
        ("5606", "1631"),
        ("5605", "1630"),
        ("5604", "1629"),
    )
    # Given data with LONGI_NOM and LATIIT_NOM but not LATIT and LONGO
    given_data = generate_data_frame_from_data_list(
        [
            {
                "SERNO": "123",
                "STATN": "Station",
                "LATIT": latitude,
                "LONGI": longitude,
                "LATIT_NOM": nominal_latitude,
                "LONGI_NOM": nominal_longitude,
            }
            for (latitude, longitude), (nominal_latitude, nominal_longitude) in zip(
                given_measured_positions, given_nominal_positions
            )
        ]
    )

    # When creating a visit
    visit = Visit(given_data)

    # Then positions will be taken from the nominal values
    assert visit.positions() == set(given_measured_positions)

    # And none of the nominal values are presented
    assert visit.positions() ^ set(given_nominal_positions)
