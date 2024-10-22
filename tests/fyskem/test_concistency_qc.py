import pytest

from ocean_data_qc.fyskem.consistency_qc import ConsistencyQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from tests.setup_methods import (
    generate_consistency_check_configuration,
    generate_data_frame,
)


@pytest.mark.parametrize(
    "given_parameter, given_value, given_other_parameters_with_values, "
    "given_upper_deviation, given_lower_deviation, expected_flag",
    (
        ("A", 1.23, {"B": 0.123, "C": 0.123}, 0, -1, QcFlag.GOOD_DATA),
        # TODO:
        #  - Lägg till rader som täcker in alla möjliga utflaggor
        #  - Lögg till uppenbara hanterbara varianter av att value är nan/None
    ),
)
def test_consistency_qc_using_override_configuration(
    given_parameter,
    given_value,
    given_other_parameters_with_values,
    given_upper_deviation,
    given_lower_deviation,
    expected_flag,
):
    # Given parameters with given values for a given depth and visit_key
    given_depth = 20
    given_visit_key = "ABC123"
    given_data = generate_data_frame(
        [
            {
                "parameter": given_parameter,
                "value": given_value,
                "DEPH": given_depth,
                "visit_key": given_visit_key,
            },
            *(
                {
                    "parameter": parameter,
                    "value": value,
                    "DEPH": given_depth,
                    "visit_key": given_visit_key,
                }
                for parameter, value in given_other_parameters_with_values.items()
            ),
        ]
    )

    # Given a consistency_qc object has been initiated with an override configuration that
    # includes given parameter
    given_other_parameters = list(given_other_parameters_with_values.keys())
    given_configuration = generate_consistency_check_configuration(
        given_parameter,
        given_other_parameters,
        given_upper_deviation,
        given_lower_deviation,
    )

    consistency_qc = ConsistencyQc(given_data)
    consistency_qc.expand_qc_columns()

    # When performing QC
    consistency_qc.check(given_parameter, given_configuration)

    # And finalizing data
    consistency_qc.collapse_qc_columns()

    # Then the automatic QC flags has at least as many positions
    # to include the field for Consistency Check
    parameter_after = Parameter(
        given_data[given_data.parameter == given_parameter].loc[0]
    )
    assert len(parameter_after.qc.automatic) >= (QcField.ConsistencyCheck + 1)

    # And the parameter is given the expected flag at the expected position
    assert parameter_after.qc.automatic[QcField.ConsistencyCheck] == expected_flag
