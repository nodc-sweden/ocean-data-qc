import pandas as pd
import pytest

from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag


@pytest.mark.parametrize(
    "given_parameter_name, given_parameter_value",
    (
        ("PH", 7.1),
        ("DEPH", 100),
        ("CPHL", 1.23),
    ),
)
def test_parameter_wraps_pandas_series(given_parameter_name, given_parameter_value):
    # Given a Series with a specific parameter value
    given_series = pd.Series(
        {"parameter": given_parameter_name, "value": given_parameter_value}
    )

    # When creating a Parameter object
    parameter = Parameter(given_series)

    # Then the given name and value can be retrieved
    assert parameter.name == given_parameter_name
    assert parameter.value == given_parameter_value


def test_parameter_sets_initial_qc_value_if_missing():
    # Given parameter data
    given_parameter_data = pd.Series({"parameter": "parameter_name", "value": 42})

    # When creating a parameter
    parameter = Parameter(given_parameter_data)

    # Then no QC has been performed
    assert parameter.qc.incoming == QcFlag.NO_QC_PERFORMED
    assert len(parameter.qc.automatic)
    assert all(flag == QcFlag.NO_QC_PERFORMED for flag in parameter.qc.automatic)
    assert parameter.qc.manual == QcFlag.NO_QC_PERFORMED
    assert parameter.qc.total == QcFlag.NO_QC_PERFORMED


def test_parameter_exposes_existing_qc_flags():
    # Given parameter data with QC_FLAG data
    given_parameter_data = pd.Series(
        {"parameter": "parameter_name", "value": 42, "quality_flag_long": "1_234_5_4"}
    )

    # When creating a parameter
    parameter = Parameter(given_parameter_data)

    # Then the QC reflects the initial data
    assert parameter.qc.incoming == QcFlag.GOOD_DATA
    assert parameter.qc.automatic == (
        QcFlag.PROBABLY_GOOD_DATA,
        QcFlag.BAD_DATA_CORRECTABLE,
        QcFlag.BAD_DATA,
    )
    assert parameter.qc.manual == QcFlag.VALUE_CHANGED
