import numpy as np
import pandas as pd
import pytest

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_configuration import QcConfiguration


def given_parameter_with_data(data: dict):
    parameter_series = pd.Series(data)
    return Parameter(parameter_series)


@pytest.mark.parametrize(
    "given_parameter_name, expected_min, expected_max",
    (
        ("ALKY", 0, 10),
        ("AMON", 0, 60),
        ("CNDC_CTD", 0, 5),
        ("CPHL", 0, 150),
        ("DEPH", 0, 1000),
        ("DOXY_BTL", 0, 12),
        ("DOXY_CTD", 0, 12),
        ("H2S", 0, 300),
        ("HUMUS", 0, 200),
        ("NTOT", 0, 100),
        ("NTRA", 0, 50),
        ("NTRI", 0, 20),
        ("NTRZ", 0, 50),
        ("PHOS", 0, 15),
        ("PRES_CTD", 0, 1000),
        ("PTOT", 0, 15),
        ("SALT_BTL", 0, 36),
        ("SALT_CTD", 0, 36),
        ("TEMP_BTL", -1, 30),
        ("TEMP_CTD", -1, 30),
        ("TOC", 0, 10),
    ),
)
def test_range_check_default_qc_configuration(
    given_parameter_name, expected_min, expected_max
):
    # When creating a configuration
    given_configuration = QcConfiguration()

    # Then the default values can be retrieved
    retrieved_configuration = given_configuration.get("range_check", given_parameter_name)

    assert retrieved_configuration.min_range_value == expected_min
    assert retrieved_configuration.max_range_value == expected_max


@pytest.mark.parametrize(
    "given_parameter_name, expected_limit",
    (
        ("ALKY", 0.5),
        ("AMON", 0.2),
        ("CPHL", 0.2),
        ("DOXY_BTL", 0.1),
        ("DOXY_CTD", 0.2),
        ("H2S", 4),
        ("NTOT", 5),
        ("NTRA", 0.1),
        ("NTRI", 0.02),
        ("NTRZ", 0.1),
    ),
)
def test_detectionlimit_check_default_qc_configuration(
    given_parameter_name, expected_limit
):
    # When creating a configuration
    given_configuration = QcConfiguration()

    # Then the default value can be retrieved
    retrieved_configuration = given_configuration.get(
        "detectionlimit_check", given_parameter_name
    )

    assert retrieved_configuration.limit == expected_limit


@pytest.mark.parametrize(
    "given_parameter_name, given_sea, given_depth, given_month, expected_min, expected_max",  # noqa: E501
    (
        ("TEMP_CTD", "Kattegat", 0, "01", -1, 10),
        ("TEMP_CTD", "Kattegat", 5, "02", -2, 10),
    ),
)
def test_statistic_check_default_qc_configuration_returns_tuple_of_floats(
    given_parameter_name, given_sea, given_depth, given_month, expected_min, expected_max
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

    assert expected_min < min_range_value < expected_max
    assert expected_min < max_range_value < expected_max


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
