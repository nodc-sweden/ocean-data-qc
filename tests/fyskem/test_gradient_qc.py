import polars as pl
import pytest

from ocean_data_qc.fyskem.gradient_qc import GradientQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from tests.setup_methods import (
    generate_data_frame,
    generate_gradient_configuration,
)


@pytest.mark.parametrize(
    "given_parameter, given_depths, given_values, "
    "allowed_decrease, allowed_increase, expected_flags",
    (
        (
            "A",
            [5, 10, 15, 20],
            [10.0, 4.99, 7.1, 14.2],
            -1,
            1,
            [
                QcFlag.NO_QUALITY_CONTROL,
                QcFlag.BAD_VALUE,
                QcFlag.GOOD_VALUE,
                QcFlag.BAD_VALUE,
            ],
        ),
    ),
)
def test_stability_qc_using_override_configuration(
    given_parameter,
    given_depths,
    given_values,
    allowed_decrease,
    allowed_increase,
    expected_flags,
):
    # Given parameters with given values for a given depth and visit_key
    given_visit_key = "ABC123"
    given_data = generate_data_frame(
        [
            *(
                {
                    "parameter": given_parameter,
                    "value": given_value,
                    "DEPH": given_depth,
                    "visit_key": given_visit_key,
                }
                for given_depth, given_value in zip(given_depths, given_values)
            ),
        ]
    )

    # Given a stability_qc object has been initiated with an override configuration that
    # includes given parameter
    given_configuration = generate_gradient_configuration(
        given_parameter,
        allowed_decrease=allowed_decrease,
        allowed_increase=allowed_increase,
    )

    gradient_qc = GradientQc(given_data)
    gradient_qc.expand_qc_columns()

    # When performing QC
    gradient_qc.check(given_parameter, given_configuration)

    # And finalizing data
    gradient_qc.collapse_qc_columns()
    given_data = gradient_qc._data
    # with pl.Config(tbl_cols=-1):
    #     print(given_data)
    # Then the automatic QC flags has at least as many positions
    # to include the field for IncreaseDecrease Check
    filtered_data = given_data.filter(pl.col("parameter") == given_parameter)
    parameter_after_list = [
        Parameter(filtered_data.row(i, named=True)) for i in range(filtered_data.height)
    ]

    assert len(parameter_after_list) == len(expected_flags)

    for parameter_after, expected_flag in zip(parameter_after_list, expected_flags):
        assert len(parameter_after.qc.automatic) >= (QcField.Gradient + 1)

        # And the parameter is given the expected flag at the expected position
        assert parameter_after.qc.automatic[QcField.Gradient] == expected_flag
