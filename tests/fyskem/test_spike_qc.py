import polars as pl
import pytest

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from ocean_data_qc.fyskem.spike_qc import SpikeQc
from tests.setup_methods import (
    generate_data_frame,
    generate_spike_configuration,
)


@pytest.mark.parametrize(
    "given_parameter, given_values, given_depths, allowed_delta, expected_flags",
    (
        (
            "DOXY",
            [1, 0.5, 7, 2],
            [0, 5, 10, 15],
            0.5,
            [
                QcFlag.NO_QC_PERFORMED,
                QcFlag.BAD_DATA_CORRECTABLE,
                QcFlag.BAD_DATA_CORRECTABLE,
                QcFlag.NO_QC_PERFORMED,
            ],
        ),
        (
            "DOXY",
            [8.85, 8.84, 9.60, 8.68],
            [0, 5, 10, 15],
            0.5,
            [
                QcFlag.NO_QC_PERFORMED,
                QcFlag.GOOD_DATA,
                QcFlag.BAD_DATA_CORRECTABLE,
                QcFlag.NO_QC_PERFORMED,
            ],
        ),
        (
            "DOXY_FLADEN_MAR_25",
            [
                8.67,
                8.57,
                7.37,
                7.27,
                7.17,
                7.26,
                7.24,
                6.88,
                6.56,
                7.04,
                6.35,
                6.38,
                6.33,
            ],
            [0, 5, 10, 15, 20, 25, 30, 40, 50, 60, 75, 80, 81],
            0.5,
            [
                QcFlag.NO_QC_PERFORMED,
                QcFlag.GOOD_DATA,
                QcFlag.GOOD_DATA,
                QcFlag.GOOD_DATA,
                QcFlag.GOOD_DATA,
                QcFlag.GOOD_DATA,
                QcFlag.GOOD_DATA,
                QcFlag.GOOD_DATA,
                QcFlag.GOOD_DATA,
                QcFlag.BAD_DATA_CORRECTABLE,
                QcFlag.GOOD_DATA,
                QcFlag.GOOD_DATA,
                QcFlag.NO_QC_PERFORMED,
            ],
        ),
    ),
)
def test_spike_qc_using_override_configuration(
    given_parameter,
    given_values,
    given_depths,
    allowed_delta,
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
                for given_value, given_depth in zip(given_values, given_depths)
            ),
        ]
    )

    # Given a consistency_qc object has been initiated with an override configuration that
    # includes given parameter
    given_configuration = generate_spike_configuration(
        given_parameter, allowed_delta=allowed_delta, allowed_depths=[]
    )
    spike_qc = SpikeQc(given_data)
    spike_qc.expand_qc_columns()

    # When performing QC
    spike_qc.check(given_parameter, given_configuration)

    # And finalizing data
    spike_qc.collapse_qc_columns()
    given_data = spike_qc._data
    # Then the automatic QC flags has at least as many positions
    # to include the field for Spike Check
    filtered_data = given_data.filter(pl.col("parameter") == given_parameter)
    parameter_after_list = [
        Parameter(filtered_data.row(i, named=True)) for i in range(filtered_data.height)
    ]
    pl.Config.set_tbl_rows(-1)  # show all rows
    pl.Config.set_tbl_cols(-1)  # show all columns
    for row in filtered_data.select(["value", "DEPH", "info_AUTO_QC_Spike"]).rows():
        print(row)
    assert zip(parameter_after_list, expected_flags, strict=True)

    for parameter_after, expected_flag in zip(parameter_after_list, expected_flags):
        print(parameter_after.value, parameter_after.depth, expected_flag)
        assert len(parameter_after.qc.automatic) >= (QcField.Spike + 1)

        # And the parameter is given the expected flag at the expected position
        assert parameter_after.qc.automatic[QcField.Spike] == expected_flag
