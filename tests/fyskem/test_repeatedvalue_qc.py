import polars as pl
import pytest

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from ocean_data_qc.fyskem.repeated_value_qc import RepeatedValueQc
from tests.setup_methods import (
    generate_data_frame,
    generate_repeatedvalue_configuration,
)


@pytest.mark.parametrize(
    "given_parameter, given_depths, given_values, repeated_value_indicator, "
    "expected_flags",
    (
        (
            "A",
            [5, 10, 15],
            [1, 1, 2],
            0,
            [
                QcFlag.GOOD_VALUE,
                QcFlag.PROBABLY_GOOD_VALUE,
                QcFlag.GOOD_VALUE,
            ],
        ),
        (
            "A",
            [5, 10, 15, 20, 25, 30, 40, 50],
            [1, 1, 2, None, 3, 4, None, 4],
            0,
            [
                QcFlag.GOOD_VALUE,
                QcFlag.PROBABLY_GOOD_VALUE,
                QcFlag.GOOD_VALUE,
                QcFlag.MISSING_VALUE,
                QcFlag.GOOD_VALUE,
                QcFlag.GOOD_VALUE,
                QcFlag.MISSING_VALUE,
                QcFlag.PROBABLY_GOOD_VALUE,
            ],
        ),
        (
            "A",
            [5, 10, 15],
            [None, 1, 2],
            0,
            [
                QcFlag.MISSING_VALUE,
                QcFlag.GOOD_VALUE,
                QcFlag.GOOD_VALUE,
            ],
        ),
    ),
)
def test_repeated_value_qc_using_override_configuration(
    given_parameter,
    given_depths,
    given_values,
    repeated_value_indicator,
    expected_flags,
):
    # Given parameters with given values for given depths and visit_key
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

    # Given a repeated object has been initiated with an override configuration that
    # includes given parameter
    given_configuration = generate_repeatedvalue_configuration(
        given_parameter,
        repeated_value=repeated_value_indicator,
    )

    repeatedvalue_qc = RepeatedValueQc(given_data)
    repeatedvalue_qc.expand_qc_columns()

    # When performing QC
    repeatedvalue_qc.check(given_parameter, given_configuration)

    # And finalizing data
    repeatedvalue_qc.collapse_qc_columns()
    given_data = repeatedvalue_qc._data

    # Then the automatic QC flags has at least as many positions
    # to include the field for the repeated value check
    filtered_data = given_data.filter(pl.col("parameter") == given_parameter)
    parameter_after_list = [
        Parameter(filtered_data.row(i, named=True)) for i in range(filtered_data.height)
    ]

    assert zip(parameter_after_list, expected_flags, strict=True)

    for parameter_after, expected_flag in zip(parameter_after_list, expected_flags):
        assert len(parameter_after.qc.automatic) >= (QcField.RepeatedValue + 1)

        # And the parameter is given the expected flag at the expected position
        assert parameter_after.qc.automatic[QcField.RepeatedValue] == expected_flag
