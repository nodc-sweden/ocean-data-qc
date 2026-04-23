import pytest

from ocean_data_qc.fyskem.h2s_qc import H2sQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from tests.setup_methods import generate_data_frame, generate_h2s_configuration


@pytest.mark.parametrize(
    "given_parameter_name, given_parameter_value, given_parameter_quality_flag_long,"
    "given_h2s_quality_flag_long, expected_flag",
    (
        (
            "NTRA",
            1.23,
            "0_00000000_0_0",
            "3_00000000_0_3",
            QcFlag.BAD_VALUE,
        ),
        (
            "NTRA",  # given_parameter_name
            1.23,  # given_parameter_value
            "3_00000000_0_3",  # given_parameter_quality_flag_long
            "1_00000000_0_1",  # given_h2s_quality_flag_long
            QcFlag.BAD_VALUE,  # return bad because h2s qflag == 1
        ),
    ),
)
def test_h2s_check_using_override_configuration(
    given_parameter_name,
    given_parameter_value,
    given_parameter_quality_flag_long,
    given_h2s_quality_flag_long,
    expected_flag,
):
    # Given parameters with given values for a given depth and visit_key
    given_depth = 20
    given_visit_key = "ABC123"
    given_data = generate_data_frame(
        [
            {
                "parameter": given_parameter_name,
                "value": given_parameter_value,
                "quality_flag_long": given_parameter_quality_flag_long,
                "DEPH": given_depth,
                "visit_key": given_visit_key,
            },
            {
                "parameter": "H2S",
                "value": 1.23,  # Any value
                "quality_flag_long": given_h2s_quality_flag_long,
                "DEPH": given_depth,
                "visit_key": given_visit_key,
            },
        ]
    )

    # Given a h2s_qc object has been initiated with an override configuration that
    # includes given parameter
    given_configuration = generate_h2s_configuration(given_parameter_name)

    h2s_qc = H2sQc(given_data)
    h2s_qc.expand_qc_columns()

    # When performing QC
    h2s_qc.check(given_parameter_name, given_configuration)

    # And finalizing data
    h2s_qc.collapse_qc_columns()
    given_data = h2s_qc._data

    # Then the automatic QC flags has at least as many positions
    # to include the field for H2S Check
    parameter_after = Parameter(given_data.row(0, named=True))
    assert len(parameter_after.qc.automatic) >= (QcField.H2s + 1)

    # And the parameter is given the expected flag at the expected position
    assert parameter_after.qc.automatic[QcField.H2s] == expected_flag
