import numpy as np
import pandas as pd
import pytest
from fyskemqc.parameter import Parameter
from fyskemqc.qc_flag import QcFlag
from fyskemqc.qc_flag_tuple import QcField
from fyskemqc.range_qc import RangeQc

from tests.setup_methods import generate_range_check_configuration


@pytest.mark.parametrize(
    "given_value, given_global_range, expected_flag",
    (
        (1.235, (1.23, 1.24), QcFlag.GOOD_DATA),
        (1.23, (1.23, 1.24), QcFlag.GOOD_DATA),  # Inclusive lower limit
        (1.24, (1.23, 1.24), QcFlag.GOOD_DATA),  # Inclusive upper limit
        (1.22999, (1.23, 1.24), QcFlag.BAD_DATA),
        (1.24001, (1.23, 1.24), QcFlag.BAD_DATA),
        (np.nan, (1.23, 1.24), QcFlag.MISSING_VALUE),
        (None, (1.23, 1.24), QcFlag.MISSING_VALUE),
    ),
)
def test_quality_flag_for_value_with_global_limits_using_override_configuration(
    given_value, given_global_range, expected_flag
):
    # Given a parameter with given value
    given_parameter_name = "parameter_name"
    parameter = Parameter(
        pd.Series({"parameter": given_parameter_name, "value": given_value})
    )

    # And no QC has been made
    assert expected_flag != QcFlag.NO_QC_PERFORMED
    assert expected_flag not in parameter.qc.automatic

    # And a limits object has been initiated with an override configuration that includes
    # given parameter
    given_configuration = generate_range_check_configuration(
        given_parameter_name, *given_global_range
    )
    limits_qc = RangeQc(given_configuration)

    # When performing QC
    limits_qc.check(parameter)

    # Then the automatic QC flags has at least as many positions
    # to include the field for Range Check
    assert len(parameter.qc.automatic) >= (QcField.RangeCheck + 1)

    # Then the parameter is given the expected flag at the expected position
    assert parameter.qc.automatic[QcField.RangeCheck] == expected_flag
