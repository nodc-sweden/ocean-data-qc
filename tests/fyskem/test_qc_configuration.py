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
def test_statistic_check_default_qc_configuration_returns_tuple_of_floats(
    given_parameter_name, given_sea, given_depth, given_month
):
    # When creating a configuration
    given_configuration = QcConfiguration()

    # get configuretion for the parameter
    retrieved_configuration = given_configuration.get(
        "statistic_check", given_parameter_name
    )

    # get thresholds for sea_area, depth, month
    (
        min_range_value,
        max_range_value,
    ) = retrieved_configuration.get_thresholds(given_sea, given_depth, given_month)
    assert isinstance(min_range_value, (int, float))
    assert isinstance(max_range_value, (int, float))


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
    min_range_value, max_range_value = retrieved_configuration.get_thresholds(
        given_sea, given_depth, given_month
    )

    assert min_range_value, max_range_value is np.nan


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
