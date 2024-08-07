import numpy as np
import pytest
from fyskemqc.detection_limit_qc import DetectionLimitQc
from fyskemqc.parameter import Parameter
from fyskemqc.qc_flag import QcFlag
from fyskemqc.qc_flag_tuple import QcField

from tests.setup_methods import (
    generate_data_frame,
    generate_detection_limit_configuration,
)


@pytest.mark.parametrize(
    "given_value, given_detection_limit, expected_flag",
    (
        (1.234, 1.233, QcFlag.GOOD_DATA),
        (1.234, 1.234, QcFlag.GOOD_DATA),
        (1.234, 1.235, QcFlag.BELOW_DETECTION),
        (np.nan, 1.234, QcFlag.MISSING_VALUE),
        (None, 1.234, QcFlag.MISSING_VALUE),
    ),
)
def test_quality_flag_for_value_with_global_limit_using_override_configuration(
    given_value, given_detection_limit, expected_flag
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
    given_configuration = generate_detection_limit_configuration(
        given_parameter_name, given_detection_limit
    )
    limits_qc = DetectionLimitQc(given_data)
    limits_qc.expand_qc_columns()

    # When performing QC
    limits_qc.check(given_parameter_name, given_configuration)

    # And finalizing data
    limits_qc.collapse_qc_columns()

    # Then the automatic QC flags has at least as many positions
    # to include the field for Range Check
    parameter_after = Parameter(given_data.loc[0])
    assert len(parameter_after.qc.automatic) >= (QcField.DetectionLimitCheck + 1)

    # And the parameter is given the expected flag at the expected position
    assert parameter_after.qc.automatic[QcField.DetectionLimitCheck] == expected_flag
