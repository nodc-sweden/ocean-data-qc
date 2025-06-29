import numpy as np
import pandas as pd
import pytest

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_configuration import QcConfiguration


def given_parameter_with_data(data: dict):
    parameter_series = pd.Series(data)
    return Parameter(parameter_series)


@pytest.mark.parametrize(
    "given_parameter_name, given_sea, given_depth, given_month",
    (
        ("TEMP_CTD", "Kattegat", 0, "01"),
        ("TEMP_CTD", "Kattegat", 5, "02"),
    ),
)
def test_statistic_check_default_qc_configuration_returns_all_thresholds(
    given_parameter_name, given_sea, given_depth, given_month
):
    # When creating a configuration
    given_configuration = QcConfiguration()

    # Get configuration for the parameter
    retrieved_configuration = given_configuration.get(
        "statistic_check", given_parameter_name
    )

    # Get all 8 thresholds
    (
        min_range_value,
        max_range_value,
        flag1_lower,
        flag1_upper,
        flag2_lower,
        flag2_upper,
        flag3_lower,
        flag3_upper,
    ) = retrieved_configuration.get_thresholds(given_sea, given_depth, given_month)

    # Assert all are int or float (but allow None if configuration allows missing)
    for threshold in (
        min_range_value,
        max_range_value,
        flag1_lower,
        flag1_upper,
        flag2_lower,
        flag2_upper,
        flag3_lower,
        flag3_upper,
    ):
        assert isinstance(threshold, (int, float)) or threshold is None


@pytest.mark.parametrize(
    "given_parameter_name, given_sea, given_depth, given_month",
    (
        ("TEMP_CTD", "unknown", 0, "01"),  # the sea_basin is not in config
        ("TEMP_CTD", "Kattegat", 1000, "02"),  # the depth is not in config
        ("TEMP_CTD", "Kattegat", 0, "13"),  # the month is not in config
    ),
)
def test_statistic_check_no_qc_configuration_for_args_returns_nan(
    given_parameter_name, given_sea, given_depth, given_month
):
    # When creating a configuration
    given_configuration = QcConfiguration()

    # get configuretion for the parameter
    retrieved_configuration = given_configuration.get(
        "statistic_check", given_parameter_name
    )
    config_tuple = retrieved_configuration.get_thresholds(
        given_sea, given_depth, given_month
    )

    assert all(np.isnan(value) for value in config_tuple)


@pytest.mark.parametrize(
    "given_parameter_name",
    (("unknown"),),
)
def test_statistic_check_unkown_parameter_returns_none(given_parameter_name):
    # When creating a configuration
    given_configuration = QcConfiguration()

    # get configuretion for the parameter
    retrieved_configuration = given_configuration.get(
        "statistic_check", given_parameter_name
    )

    assert retrieved_configuration is None
