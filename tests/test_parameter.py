import pandas as pd
import pytest
from fyskemqc.parameter import Parameter
from fyskemqc.qc_flag import QcFlag


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


def test_initial_qc_value():
    # Given parameter data
    given_parameter_data = pd.Series({"parameter": "parameter_name", "value": 42})

    # When creating a parameter
    parameter = Parameter(given_parameter_data)

    # Then no QC has been performed
    assert parameter.qc.incoming == QcFlag.NO_QC_PERFORMED
    assert parameter.qc.automatic == [QcFlag.NO_QC_PERFORMED]
    assert parameter.qc.manual == QcFlag.NO_QC_PERFORMED


def test_set_qc_value():
    # Given a parameter
    given_parameter_data = pd.Series({"parameter": "parameter_name", "value": 42})
    given_parameter = Parameter(given_parameter_data)

    # And a new QC_value has been set
    given_parameter.qc.incoming = QcFlag.GOOD_DATA
    given_parameter.qc.automatic = [
        QcFlag.PROBABLY_GOOD_DATA,
        QcFlag.BAD_DATA_CORRECTABLE,
    ]
    given_parameter.qc.manual = QcFlag.BAD_DATA

    # When retrieving the data
    data = given_parameter.data

    # Then the QC flags are set
    assert data.QC_FLAGS == "1_23_4"
