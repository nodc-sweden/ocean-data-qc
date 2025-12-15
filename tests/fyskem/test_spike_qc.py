import os

import polars as pl
import pytest

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_configuration import QcConfiguration
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from ocean_data_qc.fyskem.spike_qc import SpikeQc
from tests.setup_methods import (
    generate_data_frame,
)


@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipping test in CI environment")
@pytest.mark.parametrize(
    "given_parameter, given_parameters_with_values, given_depths, expected_flags",
    (
        (
            "DOXY_BTL",
            {"DOXY_BTL": [1, 0.5, 7, 2], "SALT_CTD": [20, 20, 20, 20]},
            [0, 5, 10, 15],
            [
                QcFlag.NO_QUALITY_CONTROL,
                QcFlag.GOOD_VALUE,  # this is hard to catch should be flag 3 but since previous is not flgged yet this will be in a steep gradient  # noqa: E501
                QcFlag.BAD_VALUE,
                QcFlag.NO_QUALITY_CONTROL,
            ],
        ),
        (
            "DOXY_BTL",  # STEVNS KLINT FEB 25
            {"DOXY_BTL": [8.59, 8.93, 8.61, 8.60], "SALT_CTD": [20, 20, 20, 20]},
            [0, 5, 10, 15],
            [
                QcFlag.NO_QUALITY_CONTROL,
                QcFlag.PROBABLY_BAD_VALUE,
                QcFlag.GOOD_VALUE,
                QcFlag.NO_QUALITY_CONTROL,
            ],
        ),
        (
            "DOXY_BTL",
            {"DOXY_BTL": [8.85, 8.84, 9.60, 8.68], "SALT_CTD": [20, 20, 20, 20]},
            [0, 5, 10, 15],
            [
                QcFlag.NO_QUALITY_CONTROL,
                QcFlag.GOOD_VALUE,
                QcFlag.PROBABLY_BAD_VALUE,
                QcFlag.NO_QUALITY_CONTROL,
            ],
        ),
        (
            "DOXY_BTL",  # _BY15_FEB_24?
            {
                "DOXY_BTL": [8.40, 8.1, 7.60, 2.4, 0.7, 0.3, 0.2],
                "SALT_CTD": [7, 7.2, 8, 9, 9.5, 10, 11],
            },
            [50, 60, 65, 70, 75, 80, 85],
            [
                QcFlag.NO_QUALITY_CONTROL,  # first value not test
                QcFlag.GOOD_VALUE,  # gradient let through
                QcFlag.GOOD_VALUE,  # gradient let through
                QcFlag.GOOD_VALUE,  # gradient let through
                QcFlag.GOOD_VALUE,  # gradient let through
                QcFlag.GOOD_VALUE,  # gradient let through
                QcFlag.NO_QUALITY_CONTROL,  # gradient let through
            ],
        ),
        (
            "DOXY_BTL",  # SLÄGGÖ FEB_25
            {
                "DOXY_BTL": [6.56, 6.31, 5.70, 5.05],
                "SALT_CTD": [29.74, 31.13, 33.04, 33.622],
            },
            [30, 40, 50, 74],
            [
                QcFlag.NO_QUALITY_CONTROL,  # first value not test
                QcFlag.GOOD_VALUE,  # gradient let through
                QcFlag.GOOD_VALUE,  # gradient let through
                QcFlag.NO_QUALITY_CONTROL,  # gradient let through
            ],
        ),
        (
            "DOXY_BTL",  # DOXY_FLADEN_MAR_25",
            {
                "DOXY_BTL": [
                    7.24,
                    6.88,
                    6.56,
                    7.04,
                    6.35,
                    6.38,
                    6.33,
                ],
                "SALT_CTD": [
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
            },
            [30, 40, 50, 60, 75, 80, 81],
            [
                QcFlag.NO_QUALITY_CONTROL,
                QcFlag.PROBABLY_BAD_VALUE,  # GOOD?
                QcFlag.GOOD_VALUE,
                QcFlag.PROBABLY_BAD_VALUE,  # hade tänkt att denna skulla flagga B
                QcFlag.GOOD_VALUE,
                QcFlag.GOOD_VALUE,
                QcFlag.NO_QUALITY_CONTROL,
            ],
        ),
    ),
)
def test_spike_qc_using_override_configuration(
    given_parameter,
    given_parameters_with_values,
    given_depths,
    expected_flags,
):
    # Given parameters with given values for a given depth and visit_key

    given_visit_key = "ABC123"
    given_data = generate_data_frame(
        [
            {
                "parameter": parameter,
                "value": value,
                "DEPH": depth,
                "visit_key": given_visit_key,
            }
            for parameter, values in given_parameters_with_values.items()
            for value, depth in zip(values, given_depths)
        ]
    )

    # Given a spike_qc object has been initiated with an override configuration that
    # includes given parameter
    configuration = QcConfiguration().get("spike_check", given_parameter)
    print(f"{configuration.threshold_high=}")
    print(f"{configuration.threshold_low=}")
    spike_qc = SpikeQc(given_data)
    spike_qc.expand_qc_columns()

    # When performing QC
    spike_qc.check(given_parameter, configuration)

    # And finalizing data
    spike_qc.collapse_qc_columns()
    given_data = spike_qc._data
    print(given_data)
    # Then the automatic QC flags has at least as many positions
    # to include the field for Spike Check
    filtered_data = given_data.filter(pl.col("parameter") == given_parameter)
    parameter_after_list = [
        Parameter(filtered_data.row(i, named=True)) for i in range(filtered_data.height)
    ]
    print(filtered_data)
    pl.Config.set_tbl_rows(-1)  # show all rows
    pl.Config.set_tbl_cols(-1)  # show all columns
    for row in filtered_data.select(["value", "DEPH", "info_AUTO_QC_Spike"]).rows():
        print(row)
    assert zip(parameter_after_list, expected_flags, strict=True)

    for parameter_after, expected_flag in zip(parameter_after_list, expected_flags):
        print(
            f"value: {parameter_after.value}, depth: {parameter_after.depth}, \
            {expected_flag=}, test result: {parameter_after.qc.automatic[QcField.Spike]}"
        )
        assert len(parameter_after.qc.automatic) >= (QcField.Spike + 1)

        # And the parameter is given the expected flag at the expected position
        assert parameter_after.qc.automatic[QcField.Spike] == expected_flag
