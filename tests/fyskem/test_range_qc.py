import numpy as np
import pytest

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from ocean_data_qc.fyskem.range_qc import RangeQc
from tests.setup_methods import generate_data_frame, generate_range_check_configuration


@pytest.mark.parametrize(
    "given_value, given_global_range, expected_flag",
    (
        (1.235, (1.23, 1.24), QcFlag.GOOD_DATA),
        (1.23, (1.23, 1.24), QcFlag.GOOD_DATA),  # Inclusive lower limit
        (1.24, (1.23, 1.24), QcFlag.GOOD_DATA),  # Inclusive upper limit
        (1.22999, (1.23, 1.24), QcFlag.BAD_DATA),
        (1.24001, (1.23, 1.24), QcFlag.BAD_DATA),
        (np.nan, (1.23, 1.24), QcFlag.MISSING_VALUE),
    ),
)
def test_quality_flag_for_value_with_global_limits_using_override_configuration(
    given_value, given_global_range, expected_flag
):
    # Given a parameter with given value
    given_parameter_name = "parameter_name"
    given_data = generate_data_frame(
        [{"parameter": given_parameter_name, "value": given_value}]
    )

    # And no QC has been made
    parameter_before = Parameter(given_data)
    assert expected_flag != QcFlag.NO_QC_PERFORMED
    assert expected_flag not in parameter_before.qc.automatic

    # And a limits object has been initiated with an override configuration that includes
    # given parameter
    given_configuration = generate_range_check_configuration(
        given_parameter_name, *given_global_range
    )
    range_qc = RangeQc(given_data)
    range_qc.expand_qc_columns()

    # When performing QC
    range_qc.check(given_parameter_name, given_configuration)

    # And finalizing data
    range_qc.collapse_qc_columns()

    # Then the automatic QC flags has at least as many positions
    # to include the field for Range Check
    parameter_after = Parameter(given_data.loc[0])
    assert len(parameter_after.qc.automatic) >= (QcField.RangeCheck + 1)

    # And the parameter is given the expected flag at the expected position
    assert parameter_after.qc.automatic[QcField.RangeCheck] == expected_flag
