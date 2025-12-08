import polars as pl
import pytest

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from ocean_data_qc.fyskem.stability_qc import StabilityQc
from tests.setup_methods import (
    generate_data_frame,
    generate_stability_configuration,
)


@pytest.mark.parametrize(
    "given_parameter, given_depths, given_values, "
    "bad_decrease, probably_bad_decrease, probably_good_decrease, expected_flags",
    (
        (
            "A",
            [5, 10, 15, 20, 30],
            [1002, 1011, 1010.98, 1010.972, 1010.968],
            -0.01,
            -0.007,
            -0.003,
            [
                QcFlag.NO_QC_PERFORMED,
                QcFlag.GOOD_DATA,
                QcFlag.BAD_DATA,
                QcFlag.BAD_DATA_CORRECTABLE,
                QcFlag.PROBABLY_GOOD_DATA,
            ],
        ),
    ),
)
def test_stability_qc_using_override_configuration(
    given_parameter,
    given_depths,
    given_values,
    bad_decrease,
    probably_bad_decrease,
    probably_good_decrease,
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
    given_configuration = generate_stability_configuration(
        given_parameter,
        bad_decrease=bad_decrease,
        probably_bad_decrease=probably_bad_decrease,
        probably_good_decrease=probably_good_decrease,
    )

    stability_qc = StabilityQc(given_data)
    stability_qc.expand_qc_columns()

    # When performing QC
    stability_qc.check(given_parameter, given_configuration)

    # And finalizing data
    stability_qc.collapse_qc_columns()
    given_data = stability_qc._data
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
        assert len(parameter_after.qc.automatic) >= (QcField.Stability + 1)

        # And the parameter is given the expected flag at the expected position
        assert parameter_after.qc.automatic[QcField.Stability] == expected_flag
