import polars as pl
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
        "LATIT_NOM": 5711.562,
        "LONGI": 1139.446,
        "LONGI_NOM": 1139.446,
        "SDATE": "2024-07-15",
        "SERNO": 123,
        "SHIPC": 123,
        "STATN": "FLADEN",
        "STDATE": "2024-09-13",
        "STIME": "13:10",
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

    # And the qc log is empty
    assert not visit.qc_log


@pytest.mark.parametrize(
    "given_field, first_value, second_value",
    (
        ("AIRPRES", 1013, 1014),
        ("COMNT_VISIT", "Gött väder!", "Regn!"),
        ("CRUISE_NO", 123, 121),
        ("CTRYID", 123, None),
        ("LATIT", 5711.562, 5711.561),
        ("LATIT_NOM", 5711.562, 5711.561),
        ("LONGI", 1139.446, 1139.44),
        ("LONGI_NOM", 1139.446, 1139.44),
        ("SERNO", 123, 321),
        ("SHIPC", 123, 0),
        ("STATN", "FLADEN", "fladen"),
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
        "LATIT_NOM": 5711.562,
        "LONGI": 1139.446,
        "LONGI_NOM": 1139.446,
        "SDATE": "2024-07-15",
        "SERNO": 123,
        "SHIPC": 123,
        "STATN": "FLADEN",
        "STIME": "13:10",
        "WADEP": 123,
        "WINDIR": 18,
        "WINSP": 1,
        given_field: first_value,
    }

    given_data = generate_data_frame_from_data_list(
        [common_values] * 10, list(range(10, 100, 10))
    )
    # Given one row contains a divergent value
    given_data = given_data.with_columns(
        pl.when(pl.arange(0, given_data.height) == 5)
        .then(pl.lit(second_value))
        .otherwise(pl.col(given_field))
        .alias(given_field)
    )

    # Given no qc has been made for common values
    visit = Visit(given_data)
    assert visit.qc[MetadataQcField.CommonValues] == MetadataFlag.NO_QC_PERFORMED

    common_values_qc = CommonValuesQc(visit)

    # When performing qc
    common_values_qc.check()

    # Then the parameter is flagged as being bad
    assert visit.qc[MetadataQcField.CommonValues] == MetadataFlag.BAD_DATA

    # And the qc log is not empty
    assert visit.qc_log

    # And the specific field is present in the qc log
    assert given_field in visit.qc_log[MetadataQcField.CommonValues]
