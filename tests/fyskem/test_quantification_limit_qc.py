import pytest

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField, QcFlagTuple
from ocean_data_qc.fyskem.qc_flags import QcFlags
from ocean_data_qc.fyskem.quantification_limit_qc import QuantificationLimitQc
from tests.setup_methods import (
    generate_data_frame,
    generate_quantification_limit_configuration,
)


@pytest.mark.parametrize(
    "given_value, given_detection_limit, given_incoming_qc, expected_flag",
    (
        (
            1.234,
            1.233,
            QcFlag.GOOD_VALUE,
            QcFlag.GOOD_VALUE,
        ),  # Strictly above quantification limit
        (
            1.234,
            1.234,
            QcFlag.GOOD_VALUE,
            QcFlag.GOOD_VALUE,
        ),  # On quantification limit delivered as good
        (
            1.234,
            1.235,
            QcFlag.GOOD_VALUE,
            QcFlag.VALUE_BELOW_LIMIT_OF_QUANTIFICATION,
        ),  # Strictly below quantification limit
        (
            1.234,
            1.234,
            QcFlag.NO_QUALITY_CONTROL,
            QcFlag.VALUE_BELOW_LIMIT_OF_QUANTIFICATION,
        ),
    ),
)

# TODO:
# - a test that includes incoming flag

def test_quality_flag_with_global_limit_using_override_configuration(
    given_value, given_detection_limit, given_incoming_qc, expected_flag
):
    # Given a parameter with given value
    given_parameter_name = "parameter_name"
    given_manual_qc = given_incoming_qc
    given_qc = QcFlags(given_incoming_qc, QcFlagTuple(), given_manual_qc)
    given_data = generate_data_frame(
        [
            {
                "parameter": given_parameter_name,
                "value": given_value,
                "LMQNT_VAL": given_detection_limit,
                "quality_flag_long": str(given_qc),
            }
        ]
    )

    # And no QC has been made
    parameter_before = Parameter(given_data.row(0, named=True))
    assert expected_flag != QcFlag.NO_QUALITY_CONTROL
    assert expected_flag not in parameter_before.qc.automatic

    # And a limits object has been initiated with an override configuration that includes
    # given parameter
    given_configuration = generate_quantification_limit_configuration(
        given_parameter_name, given_detection_limit
    )
    limits_qc = QuantificationLimitQc(given_data)
    limits_qc.expand_qc_columns()

    # When performing QC
    limits_qc.check(given_parameter_name, given_configuration)

    # And finalizing data
    limits_qc.collapse_qc_columns()
    given_data = limits_qc._data
    # Then the automatic QC flags has at least as many positions
    # to include the field for Range Check
    parameter_after = Parameter(given_data.row(0, named=True))
    assert len(parameter_after.qc.automatic) >= (QcField.QuantificationLimit + 1)

    # And the parameter is given the expected flag at the expected position
    assert parameter_after.qc.automatic[QcField.QuantificationLimit] == expected_flag


@pytest.mark.parametrize(
    "given_value, given_detection_limit, given_incoming_qc, expected_flag",
    (
        (
            1.234,
            1.235,
            QcFlag.GOOD_VALUE,
            QcFlag.VALUE_BELOW_LIMIT_OF_QUANTIFICATION,
        ),  # Strictly below quantification limit
        (
            1.234,
            1.234,
            QcFlag.NO_QUALITY_CONTROL,
            QcFlag.VALUE_BELOW_LIMIT_OF_QUANTIFICATION,
        ),
    ),
)
def test_quality_flag_for_value_with_global_limit_using_override_configuration(
    given_value, given_detection_limit, given_incoming_qc, expected_flag
):
    # Given a parameter with given value
    given_parameter_name = "parameter_name"
    given_manual_qc = given_incoming_qc
    given_qc = QcFlags(given_incoming_qc, QcFlagTuple(), given_manual_qc)
    given_data = generate_data_frame(
        [
            {
                "parameter": given_parameter_name,
                "value": given_value,
                "LMQNT_VAL": given_detection_limit,
                "quality_flag_long": str(given_qc),
            }
        ]
    )

    # And no QC has been made
    parameter_before = Parameter(given_data.row(0, named=True))
    assert expected_flag != QcFlag.NO_QUALITY_CONTROL
    assert expected_flag not in parameter_before.qc.automatic

    # And a limits object has been initiated with an override configuration that includes
    # given parameter
    given_configuration = generate_quantification_limit_configuration(
        given_parameter_name, given_detection_limit
    )
    limits_qc = QuantificationLimitQc(given_data)
    limits_qc.expand_qc_columns()

    # When performing QC
    limits_qc.check(given_parameter_name, given_configuration)

    # And finalizing data
    limits_qc.collapse_qc_columns()
    given_data = limits_qc._data
    # Then the automatic QC flags has at least as many positions
    # to include the field for Range Check
    parameter_after = Parameter(given_data.row(0, named=True))
    # And the parameter is given the expected flag at the expected position
    qc_flag = parameter_after.qc.automatic[QcField.QuantificationLimit]
    assert qc_flag == expected_flag

    # Also check that the flag value is "Q" when expected
    if expected_flag == QcFlag.VALUE_BELOW_LIMIT_OF_QUANTIFICATION:
        assert qc_flag.value == "Q"
