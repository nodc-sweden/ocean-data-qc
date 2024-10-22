import numpy as np
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
        (
            "A",
            1.23,
            {"B": 0.123, "C": 0.123},
            0,
            -1,
            QcFlag.GOOD_DATA,
        ),  # 1.23-(0.123+0.123)=0.984 vilket är >= 0
        (
            "A",
            1,
            {"B": 0.5, "C": 0.5},
            0,
            -1,
            QcFlag.GOOD_DATA,
        ),  # 1-(0.5+0.5)=0 vilket är >= 0
        (
            "A",
            1,
            {"B": 1, "C": 0.5},
            0,
            -1,
            QcFlag.PROBABLY_GOOD_DATA,
        ),  # 1-(1+0.5)=-0.5 vilket är > -1
        ("A", 1, {"B": 1, "C": 2}, 0, -1, QcFlag.BAD_DATA),  # 1-(1+2)=-2 vilket är < -1
        ("A", np.nan, {"B": 1, "C": 2}, 0, -1, QcFlag.MISSING_VALUE),
        (
            "A",
            1,
            {"B": np.nan, "C": 2},
            0,
            -1,
            QcFlag.PROBABLY_GOOD_DATA,
        ),  # 1-(2)=-1 vilket är >= -1
        # TODO:
        #  - Lägg till hantering av att alla parametrar i parameter list saknas
        # ("A", 1, {"B": None, "C": None}, 0, -1, QcFlag.NO_QC_PERFORMED), # alla parametrar i parameterlist ska returnera None från consistency_qc # noqa: E501
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
