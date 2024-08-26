import pytest
from ocean_data_qc.errors import VisitError
from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField
from ocean_data_qc.metadata.visit import Visit
from setup_methods import generate_data_frame_from_data_list


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


def test_visit_raises_if_series_id_not_unique():
    # Given a dataframe with data for multiple visits to a given station
    given_station = "Mariana trench"
    given_data = generate_data_frame_from_data_list(
        [
            {"SERNO": "001", "STATN": given_station},
            {"SERNO": "001", "STATN": given_station},
            {"SERNO": "002", "STATN": given_station},
        ]
    )

    # When creating a visit
    # Then there is an error
    with pytest.raises(VisitError):
        Visit(given_data)


def test_visit_raises_if_station_not_unique():
    # Given a dataframe with data for a visit with multiple stations
    given_series_id = "123"
    given_data = generate_data_frame_from_data_list(
        [
            {"SERNO": given_series_id, "STATN": "Yavin"},
            {"SERNO": given_series_id, "STATN": "Echo"},
            {"SERNO": given_series_id, "STATN": "Yavin"},
        ]
    )

    # When creating a visit
    # Then there is an error
    with pytest.raises(VisitError):
        Visit(given_data)


def test_visit_initializes_all_metadata_qc_flags_as_not_performed():
    given_data = generate_data_frame_from_data_list([{"SERNO": "123", "STATN": "ABC"}])

    visit = Visit(given_data)

    # Then all metadata is set to 'No QC performed'
    assert len(MetadataQcField)
    for category in MetadataQcField:
        assert visit.qc[category] == MetadataFlag.NO_QC_PERFORMED


def test_metadata_value_not_unique_is_flagged(): ...
