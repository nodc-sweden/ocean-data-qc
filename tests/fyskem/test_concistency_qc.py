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


@pytest.mark.parametrize(
    "given_parameter, given_value, given_std_uncert, given_parameters, "
    "given_values, given_std_uncerts, given_parameters_sets,"
    "given_sigma, given_upper_limit, expected_flag",
    (
        (
            "NTOT",
            14.23,
            1.35,
            ["NTRA", "NTRI", "AMON"],
            [8.24, 0.87, 3.76],
            [0.2, 0.12, 0.15],
            [["NTRZ", "AMON"], ["NTRA", "NTRI", "AMON"]],
            0,
            999,
            QcFlag.GOOD_VALUE,
        ),  # 14.23-(8.24+0.87+3.76)=1.36 vilket är >= 0
        (
            "NTOT",
            12.23,
            1.35,
            ["NTRA", "NTRI", "AMON"],
            [8.24, 0.87, 3.76],
            [0.2, 0.12, 0.15],
            [["NTRZ", "AMON"], ["NTRA", "NTRI", "AMON"]],
            0,
            999,
            QcFlag.BAD_VALUE,
        ),  # 12.23-(8.24+0.87+3.76)=-0.64 vilket är < 0
        (
            "NTOT",
            12.23,
            1.35,
            ["NTRZ", "NTRA", "NTRI", "AMON"],
            [6.24, 8.24, 0.87, 3.76],
            [0.2, 0.12, 0.15],
            [["NTRZ", "AMON"], ["NTRA", "NTRI", "AMON"]],
            0,
            999,
            QcFlag.GOOD_VALUE,
        ),  # 12.23-(6.24+3.76)=2.23 vilket är >= 0
        (
            "PTOT",
            4.23,
            1.35,
            ["PHOS"],
            [0.23],
            [0.08],
            [["PHOS"]],
            0,
            999,
            QcFlag.GOOD_VALUE,
        ),  # 4.23-0.23=4 vilket är >= 0
        (
            "PTOT",
            1.23,
            1.35,
            ["PHOS"],
            [2.23],
            [0.08],
            [["PHOS"]],
            0,
            999,
            QcFlag.BAD_VALUE,
        ),  # 1.23-2.23=-1 vilket är < 0
        (
            "PTOT",
            1023.23,
            1.35,
            ["PHOS"],
            [2.23],
            [0.08],
            [["PHOS"]],
            0,
            999,
            QcFlag.BAD_VALUE,
        ),  # 1023.23-2.23=1021 vilket är > 999
        (
            "TOC",
            12.3,
            4.35,
            ["DOC", "POC"],
            [463, 46],
            [110, 14],
            [["DOC", "POC"]],
            0,
            999,
            QcFlag.GOOD_VALUE,
        ),
        # TODO:
        #  - Lägg till hantering av att alla parametrar i parameter list saknas
        # ("TOT", 1, {"INORG_1": None, "INORG_2": None}, 0, -1, QcFlag.NO_QC_PERFORMED), # alla parametrar i parameterlist ska returnera None från consistency_qc # noqa: E501
    ),
)
def test_consistency_qc_using_override_configuration(
    given_parameter,
    given_value,
    given_std_uncert,
    given_parameters,
    given_values,
    given_std_uncerts,
    given_parameters_sets,
    given_sigma,
    given_upper_limit,
    expected_flag,
):
    # Given parameters with given values for a given depth and visit_key
    given_depth = 20
    given_visit_key = "ABC123"
    given_row_id = 1
    given_data = generate_data_frame(
        [
            # Huvudparametern
            {
                "parameter": given_parameter,
                "value": given_value,
                "STD_UNCERT": given_std_uncert,
                "DEPH": given_depth,
                "visit_key": given_visit_key,
                "_row_id": given_row_id,
            },
            *[
                {
                    "parameter": param,
                    "value": val,
                    "STD_UNCERT": std_uncert,
                    "DEPH": given_depth,
                    "visit_key": given_visit_key,
                    "_row_id": given_row_id,
                }
                for param, val, std_uncert in zip(
                    given_parameters, given_values, given_std_uncerts
                )
            ],
        ]
    )
    print(given_data)

    # Given a consistency_qc object has been initiated with an override configuration that
    # includes given parameter
    given_configuration = generate_consistency_check_configuration(
        parameter_sets=given_parameters_sets,
        sigma=given_sigma,
        upper_limit=given_upper_limit,
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
