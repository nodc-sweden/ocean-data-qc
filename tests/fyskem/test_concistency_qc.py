import numpy as np
import polars as pl
import pytest

from ocean_data_qc.fyskem.consistency_qc import ConsistencyQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from tests.setup_methods import (
    generate_consistency_check_configuration,
    generate_data_frame,
)

"given_good_lower", "given_good_upper", "given_max_lower", "given_max_upper"


@pytest.mark.parametrize(
    "given_parameter, given_value, given_other_parameters_with_values, "
    "given_good_lower, given_good_upper, given_max_lower, given_max_upper, expected_flag",
    (
        (
            "TOT",
            1.23,
            {"INORG_1": 0.123, "INORG_2": 0.123},
            999,
            -0.05,
            0,
            -1,
            QcFlag.GOOD_DATA,
        ),  # 1.23-(0.123+0.123)=0.984 vilket är >= 0
        (
            "TOT",
            1,
            {"INORG_1": 0.5, "INORG_2": 0.5},
            0,
            -0.05,
            0,
            -1,
            QcFlag.GOOD_DATA,
        ),  # 1-(0.5+0.5)=0 vilket är >= 0
        (
            "TOT",
            1,
            {"INORG_1": 1, "INORG_2": 0.5},
            0,
            -0.05,
            0,
            -1,
            QcFlag.BAD_DATA_CORRECTABLE,
        ),  # 1-(1+0.5)=-0.5 vilket är > -1
        (
            "TOT",
            1,
            {"INORG_1": 1, "INORG_2": 2},
            0,
            -0.05,
            0,
            -1,
            QcFlag.BAD_DATA,
        ),  # 1-(1+2)=-2 vilket är < -1
        (
            "TOT",
            1,
            {"INORG_1": 3},
            0,
            -0.05,
            0,
            -1,
            QcFlag.BAD_DATA,
        ),  # 1-(3)=-2 vilket är < -1
        (
            "TOT",
            1,
            {"INORG_1": 1, "INORG_2": 0.1, "D": 0.1},
            0,
            -0.05,
            0,
            -1,
            QcFlag.BAD_DATA_CORRECTABLE,
        ),  # 1-(1+0.1+0.1)=-0.2 vilket är > -1
        (
            "TOT",
            np.nan,
            {"INORG_1": 1, "INORG_2": 2},
            0,
            -0.05,
            0,
            -1,
            QcFlag.MISSING_VALUE,
        ),
        (
            "TOT",
            1,
            {"INORG_1": np.nan, "INORG_2": 2},
            0,
            -0.05,
            0,
            -1.0,
            QcFlag.BAD_DATA_CORRECTABLE,
        ),  # 1-(2)=-1 which is >= -1
        (
            "TOT",
            1,
            {"INORG_1": np.nan, "INORG_2": np.nan},
            0,
            -0.05,
            0,
            -1,
            QcFlag.NO_QC_PERFORMED,
        ),  # 1-(np.nan)=1 vilket är >=0
        (
            "TOT",
            np.nan,
            {"INORG_1": np.nan, "INORG_2": np.nan},
            0,
            -0.05,
            0,
            -1,
            QcFlag.MISSING_VALUE,
        ),  # 1-(np.nan)=1 vilket är >=0
        (
            "TOT",
            1,
            {},
            0,
            -0.05,
            0,
            -1,
            QcFlag.NO_QC_PERFORMED,
        ),
        (
            "CTD",
            1,
            {"BTL": 3},
            0.4,
            -0.4,
            1,
            -1,
            QcFlag.BAD_DATA,
        ),  # 3-1=2 vilket är >=1
        (
            "CTD",
            1,
            {"BTL": 1.5},
            0.4,
            -0.4,
            1,
            -1,
            QcFlag.BAD_DATA_CORRECTABLE,
        ),  # 1.5-1=0.5 vilket är >=0.4 men mindre än 1
        (
            "CTD",
            1,
            {"BTL": 1.3},
            0.4,
            -0.4,
            1,
            -1,
            QcFlag.GOOD_DATA,
        ),  # 1.3-1=0.3 vilket är <=0.4 vilket är godkänt
        # TODO:
        #  - Lägg till hantering av att alla parametrar i parameter list saknas
        # ("TOT", 1, {"INORG_1": None, "INORG_2": None}, 0, -1, QcFlag.NO_QC_PERFORMED), # alla parametrar i parameterlist ska returnera None från consistency_qc # noqa: E501
    ),
)
def test_consistency_qc_using_override_configuration(
    given_parameter,
    given_value,
    given_other_parameters_with_values,
    given_good_upper,
    given_good_lower,
    given_max_upper,
    given_max_lower,
    expected_flag,
):
    # Given parameters with given values for a given depth and visit_key
    given_depth = 20
    given_visit_key = "ABC123"
    given_data = generate_data_frame(
        [
            {
                "parameter": given_parameter,
                "value": given_value,
                "DEPH": given_depth,
                "visit_key": given_visit_key,
            },
            *(
                {
                    "parameter": parameter,
                    "value": value,
                    "DEPH": given_depth,
                    "visit_key": given_visit_key,
                }
                for parameter, value in given_other_parameters_with_values.items()
            ),
        ]
    )
    # Given a consistency_qc object has been initiated with an override configuration that
    # includes given parameter
    given_other_parameters = list(given_other_parameters_with_values.keys())
    good_lower = min(given_good_lower, given_good_upper)
    good_upper = max(given_good_lower, given_good_upper)
    max_lower = min(given_max_lower, given_max_upper)
    max_upper = max(given_max_lower, given_max_upper)
    given_configuration = generate_consistency_check_configuration(
        given_parameter,
        given_other_parameters,
        max_upper,
        max_lower,
        good_upper,
        good_lower,
    )
    consistency_qc = ConsistencyQc(given_data)
    consistency_qc.expand_qc_columns()

    # When performing QC
    consistency_qc.check(given_parameter, given_configuration)

    # And finalizing data
    consistency_qc.collapse_qc_columns()
    given_data = consistency_qc._data

    # Then the automatic QC flags has at least as many positions
    # to include the field for Consistency Check
    parameter_after = Parameter(
        given_data.filter(pl.col("parameter") == given_parameter).row(0, named=True)
    )
    assert len(parameter_after.qc.automatic) >= (QcField.Consistency + 1)

    # And the parameter is given the expected flag at the expected position
    assert parameter_after.qc.automatic[QcField.Consistency] == expected_flag
