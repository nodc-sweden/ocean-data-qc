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


def test_visit_handles_non_unique_series_id():
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


def test_visit_raises_if_station_not_unique():
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
