import pytest
from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField
from ocean_data_qc.metadata.visit import Visit
from ocean_data_qc.metadata.wadep_qc import WadepQc
from setup_methods import generate_data_frame_from_data_list


@pytest.mark.parametrize(
    "given_wadep, given_parameter_depths, expected_flag",
    (
        (100, (10, 20, 30), MetadataFlag.GOOD_DATA),
        (100, (10, 99.9, 30), MetadataFlag.GOOD_DATA),
        (100, (100, 10, 10), MetadataFlag.BAD_DATA),
        (100, (101, 102, 103), MetadataFlag.BAD_DATA),
        (100, (10, 20, 130), MetadataFlag.BAD_DATA),
    ),
)
def test_quality_flag_for_metadata(given_wadep, given_parameter_depths, expected_flag):
    # Given data with given water depth and given parameter depths
    given_data = generate_data_frame_from_data_list(
        [
            {"SERNO": "123", "STATN": "ABC", "WADEP": given_wadep, "DEPH": depth}
            for depth in given_parameter_depths
        ]
    )

    # Given no qc has been made for wadep
    visit = Visit(given_data)
    assert visit.qc[MetadataQcField.Wadep] == MetadataFlag.NO_QC_PERFORMED

    wadep_qc = WadepQc(visit)

    # When performing QC
    wadep_qc.check()

    # Then the parameter is given the expected flag at the expected position
    assert visit.qc[MetadataQcField.Wadep] == expected_flag
