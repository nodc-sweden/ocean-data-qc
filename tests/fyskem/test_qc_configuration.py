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
        (
            "ALKY",
            0.5,
        ),
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
def test_limit_detection_check_default_qc_configuration(
    given_parameter_name, expected_limit
):
    # When creating a configuration
    given_configuration = QcConfiguration()

    # Then the default value can be retrieved
    retrieved_configuration = given_configuration.get(
        "detection_limit_check", given_parameter_name
    )

    assert retrieved_configuration.limit == expected_limit
