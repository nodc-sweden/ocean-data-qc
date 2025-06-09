import numpy as np
import pytest

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from ocean_data_qc.fyskem.statistic_qc import StatisticQc
from tests.setup_methods import (
    generate_data_frame,
    generate_statistic_check_configuration,
)


@pytest.mark.parametrize(
    "given_value, given_range, given_month, given_depth, expected_flag",
    (
        (10, (1, 10), "01", 0, QcFlag.GOOD_DATA),
        (1, (1, 10), "01", 1, QcFlag.GOOD_DATA),
        (5, (np.nan, 10), "01", 2, QcFlag.NO_QC_PERFORMED),
        # (5, (np.nan, np.nan), None, None, QcFlag.NO_QC_PERFORMED),
        (1, (1, 10), "01", 1000, QcFlag.NO_QC_PERFORMED),
        (1, (1, 10), "13", 1000, QcFlag.NO_QC_PERFORMED),
        (0.9, (1, 10), "01", 0, QcFlag.BAD_DATA),
        (-1, (1, 10), "01", 0, QcFlag.BAD_DATA),
        (np.nan, (1, 10), "01", 1, QcFlag.MISSING_VALUE),
    ),
)
def test_quality_flag_for_value_month_depth_with_given_qc(
    given_value, given_range, given_month, given_depth, expected_flag
):
    # Given a parameter with given value
    given_parameter_name = "parameter_name"
    given_sea_area = "ocean1"
    given_data = generate_data_frame(
        [
            {
                "parameter": given_parameter_name,
                "value": given_value,
                "sea_basin": given_sea_area,
                "DEPH": given_depth,
                "visit_month": given_month,
            }
        ]
    )

    # And no QC has been made
    # Denna check ligger sedan tidigare i de andra testerna, men jag förstår inte varför
    # Det ska vara ok att köra ett till auto test efter att ett eller flera redan är körda
    # parameter_before = Parameter(given_data)
    # assert expected_flag != QcFlag.NO_QC_PERFORMED
    # varför vill vi inte att expected flag ska finnas in en redan körd qc?
    # det känns inte helt rätt.
    # assert expected_flag not in parameter_before.qc.automatic

    # Define test input values
    given_min_depth = 0
    given_max_depth = 2
    given_month = "01"

    # Create dictionary for months
    given_months = {
        given_month: {
            "min_range_value": given_range[0],
            "max_range_value": given_range[1],
        },
        "02": {"min_range_value": 0, "max_range_value": 5},
    }

    # Generate test configuration
    given_configuration = generate_statistic_check_configuration(
        sea_basin=given_sea_area,
        depth_intervals=[(given_min_depth, given_max_depth, given_months)],
    )

    statistic_qc = StatisticQc(given_data)
    statistic_qc.expand_qc_columns()
    # When performing QC
    statistic_qc.check(given_parameter_name, given_configuration)
    # And finalizing data
    statistic_qc.collapse_qc_columns()

    # Then the automatic QC flags has at least as many positions
    # to include the field for Range Check
    parameter_after = Parameter(given_data.loc[0])
    assert len(parameter_after.qc.automatic) >= (QcField.Statistic + 1)

    # And the parameter is given the expected flag at the expected position
    assert parameter_after.qc.automatic[QcField.Statistic] == expected_flag
