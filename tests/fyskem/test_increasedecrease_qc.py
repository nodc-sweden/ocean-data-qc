import numpy as np
import polars as pl
import pytest

from ocean_data_qc.fyskem.increasedecrease_qc import IncreaseDecreaseQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from tests.setup_methods import (
    generate_data_frame,
    generate_increasedecrease_configuration,
)


@pytest.mark.parametrize(
    "given_parameter, given_depths, given_values, "
    "allowed_increase, allowed_decrease, expected_flags",
    (
        (
            "A",
            [5, 10, 15],
            [1, 0.5, np.nan, 2],
            5,
            0.1,
            [
                QcFlag.GOOD_DATA,
                QcFlag.BAD_DATA,
                QcFlag.MISSING_VALUE,
                QcFlag.GOOD_DATA,
            ],
        ),
    ),
)
def test_increasedecrease_qc_using_override_configuration(
    given_parameter,
    given_depths,
    given_values,
    allowed_increase,
    allowed_decrease,
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

    # Given a consistency_qc object has been initiated with an override configuration that
    # includes given parameter
    given_configuration = generate_increasedecrease_configuration(
        given_parameter,
        allowed_decrease=allowed_decrease,
        allowed_increase=allowed_increase,
    )

    increaseadecrease_qc = IncreaseDecreaseQc(given_data)
    increaseadecrease_qc.expand_qc_columns()

    # When performing QC
    increaseadecrease_qc.check(given_parameter, given_configuration)

    # And finalizing data
    increaseadecrease_qc.collapse_qc_columns()
    given_data = increaseadecrease_qc._data
    # with pl.Config(tbl_cols=-1):
    #     print(given_data)
    # Then the automatic QC flags has at least as many positions
    # to include the field for IncreaseDecrease Check
    filtered_data = given_data.filter(pl.col("parameter") == given_parameter)
    parameter_after_list = [
        Parameter(filtered_data.row(i, named=True)) for i in range(filtered_data.height)
    ]

    assert zip(parameter_after_list, expected_flags, strict=True)

    for parameter_after, expected_flag in zip(parameter_after_list, expected_flags):
        assert len(parameter_after.qc.automatic) >= (QcField.IncreaseDecrease + 1)

        # And the parameter is given the expected flag at the expected position
        assert parameter_after.qc.automatic[QcField.IncreaseDecrease] == expected_flag
