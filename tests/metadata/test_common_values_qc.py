import pytest
from ocean_data_qc.metadata.common_values_qc import CommonValuesQc
from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField
from ocean_data_qc.metadata.visit import Visit

from tests.setup_methods import generate_data_frame_from_data_list


def test_common_metadata_with_same_value_is_flagged_as_good():
    # Given data with same value for all common metadata fields
    common_values = {
        "AIRPRES": 1013,
        "AIRTEMP": 24,
        "COMNT_VISIT": "Gött väder!",
        "CRUISE_NO": 123,
        "CTRYID": 123,
        "LATIT": 5711.562,
        "LONGI": 1139.446,
        "SDATE": "2024-07-15",
        "SHIPC": 123,
        "STATN": "FLADEN",
        "STIME": "13:10",
        "SERNO": 123,
        "WADEP": 123,
        "WINDIR": 18,
        "WINSP": 1,
    }

    given_data = generate_data_frame_from_data_list(
        [common_values] * 10, list(range(10, 100, 10))
    )

    # Given no qc has been made for common values
    visit = Visit(given_data)
    assert visit.qc[MetadataQcField.CommonValues] == MetadataFlag.NO_QC_PERFORMED

    common_values_qc = CommonValuesQc(visit)

    # When performing qc
    common_values_qc.check()

    # Then the parameter is flagged as being good
    assert visit.qc[MetadataQcField.CommonValues] == MetadataFlag.GOOD_DATA


@pytest.mark.parametrize(
    "given_field, first_value, second_value",
    (
        ("AIRPRES", 1013, 1014),
        ("COMNT_VISIT", "Gött väder!", "Regn!"),
        ("CRUISE_NO", 123, 121),
        ("CTRYID", 123, None),
        ("LATIT", 5711.562, 5711.561),
        ("LONGI", 1139.446, 1139.44),
        ("SHIPC", 123, 0),
        ("STATN", "FLADEN", "fladen"),
        ("SERNO", 123, 321),
        ("WADEP", 123, 121),
        ("WINDIR", 18, 17),
        ("WINSP", 1, 10),
    ),
)
def test_common_metadata_with_different_values_are_flagged_as_bad(
    given_field,
    first_value,
    second_value,
):
    # Given data with same value for all common metadata fields
    common_values = {
        "AIRPRES": 1013,
        "AIRTEMP": 24,
        "COMNT_VISIT": "Gött väder!",
        "CRUISE_NO": 123,
        "CTRYID": 123,
        "LATIT": 5711.562,
        "LONGI": 1139.446,
        "SDATE": "2024-07-15",
        "SHIPC": 123,
        "STATN": "FLADEN",
        "STIME": "13:10",
        "SERNO": 123,
        "WADEP": 123,
        "WINDIR": 18,
        "WINSP": 1,
        given_field: first_value,
    }

    given_data = generate_data_frame_from_data_list(
        [common_values] * 10, list(range(10, 100, 10))
    )

    # Given one row contains a divergent value
    given_data._set_value(len(given_data) // 2, given_field, second_value)

    # Given no qc has been made for common values
    visit = Visit(given_data)
    assert visit.qc[MetadataQcField.CommonValues] == MetadataFlag.NO_QC_PERFORMED

    common_values_qc = CommonValuesQc(visit)

    # When performing qc
    common_values_qc.check()

    # Then the parameter is flagged as being bad
    assert visit.qc[MetadataQcField.CommonValues] == MetadataFlag.BAD_DATA
