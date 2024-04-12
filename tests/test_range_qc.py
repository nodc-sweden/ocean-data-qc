import numpy as np
import pandas as pd
import pytest
from fyskemqc.parameter import Parameter
from fyskemqc.qc_checks import RangeCheck
from fyskemqc.qc_configuration import QcConfiguration
from fyskemqc.qc_flag import QcFlag
from fyskemqc.range_qc import RangeQc


def generate_configuration(parameter: str, min_range: float, max_range: float):
    parameter_configuration = RangeCheck(
        min_range_value=min_range, max_range_value=max_range
    )
    return QcConfiguration({parameter: {"global": parameter_configuration}})


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
    assert expected_flag not in parameter.qc.automatic

    # And a limits object has been initiated with an override configuration that includes
    # given parameter
    given_override_configuration = generate_configuration(
        given_parameter_name, *given_global_range
    )
    limits_qc = RangeQc(given_override_configuration.get(parameter))

    # When performing QC
    limits_qc.check(parameter)

    # Then the parameter is given the expected flag
    assert expected_flag in parameter.qc.automatic
