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
            "NTRZ",
            8.23,
            0.35,
            ["NTRA", "NTRI"],
            [8.20, 0.03],
            [0.35, 0.01],
            [["NTRA", "NTRI"]],
            0,
            99,
            QcFlag.GOOD_VALUE,
        ),  # 8.23-(8.2+0.03)=0
        (
            "NTRZ",
            8.24,
            0.35,
            ["NTRA", "NTRI"],
            [8.20, 0.03],
            [0.35, 0.01],
            [["NTRA", "NTRI"]],
            0,
            99,
            QcFlag.BAD_VALUE,
        ),  # 8.24-(8.2+0.03)=0.1, 0.1 > 0
        (
            "NTRZ",
            8.22,
            0.35,
            ["NTRA", "NTRI"],
            [8.20, 0.03],
            [0.35, 0.01],
            [["NTRA", "NTRI"]],
            0,
            99,
            QcFlag.BAD_VALUE,
        ),  # 8.22-(8.2+0.03)=-0.1, -0.1 > 0
        (
            "NTOT",
            14.23,
            1.35,
            ["NTRA", "NTRI", "AMON"],
            [8.24, 0.87, 3.76],
            [0.2, 0.12, 0.15],
            [["NTRZ", "AMON"], ["NTRA", "NTRI", "AMON"]],
            1.4,
            999,
            QcFlag.GOOD_VALUE,
        ),  # 14.23-(8.24+0.87+3.76)=1.36 vilket är >= -2*1.4
        (
            "NTOT",
            7.23,
            1.35,
            ["NTRA", "NTRI", "AMON"],
            [8.24, 0.87, 3.76],
            [0.2, 0.12, 0.15],
            [["NTRZ", "AMON"], ["NTRA", "NTRI", "AMON"]],
            1.4,
            999,
            QcFlag.BAD_VALUE,
        ),  # 7.23-(8.24+0.87+3.76)=-5.64 vilket är < -3*1.4
        (
            "NTOT",
            12.23,
            1.35,
            ["NTRZ", "NTRA", "NTRI", "AMON"],
            [6.24, 8.24, 0.87, 3.76],
            [0.2, 0.12, 0.15],
            [["NTRZ", "AMON"], ["NTRA", "NTRI", "AMON"]],
            1.4,
            999,
            QcFlag.GOOD_VALUE,
        ),  # 12.23-(6.24+3.76)=2.23 vilket är >= -2*1.4
        (
            "PTOT",
            4.23,
            0.5,
            ["PHOS"],
            [0.23],
            [0.08],
            [["PHOS"]],
            0.1,
            999,
            QcFlag.GOOD_VALUE,
        ),  # 4.23-0.23=4 vilket är >= -2*0.5
        (
            "PTOT",
            0.23,
            0.5,
            ["PHOS"],
            [2.23],
            [0.08],
            [["PHOS"]],
            0.1,
            99,
            QcFlag.BAD_VALUE,
        ),  # 0.23-2.23=-2 vilket är < -3*0.5
        (
            "PTOT",
            123.23,
            1.35,
            ["PHOS"],
            [2.23],
            [0.08],
            [["PHOS"]],
            0.1,
            99,
            QcFlag.BAD_VALUE,
        ),  # 123.23-2.23=121 vilket är > 99
        (
            "TOC",
            12.3,
            4.35,
            ["DOC", "POC"],
            [463, 46],
            [110, 14],
            [["DOC", "POC"]],
            70,
            999,
            QcFlag.GOOD_VALUE,
        ),  # 12*83.25701-(463+46) = 515
        (
            "TOC",
            1,
            0.3,
            ["DOC", "POC"],
            [463, 46],
            [110, 14],
            [["DOC", "POC"]],
            70,
            999,
            QcFlag.BAD_VALUE,
        ),  # 83.25701-(463+46) = -425.7, < 3*113.7
        (
            "TOC",
            5,
            0.3,
            ["DOC", "POC"],
            [300, None],
            [110, None],
            [["DOC", "POC"]],
            70,
            999,
            QcFlag.GOOD_VALUE,
        ),  # 5*83.25701-300 = 116
        (
            "TOC",
            5,
            0.3,
            ["DOC", "POC"],
            [700, None],
            [110, None],
            [["DOC", "POC"]],
            70,
            999,
            QcFlag.PROBABLY_BAD_VALUE,
        ),  # 5*83.25701-800 = -384, < -113*2 (3*sigma), >= -113*3
        (
            "TOC",
            1,
            0.3,
            ["DOC", "POC"],
            [250, 30],
            [None, None],
            [["DOC", "POC"]],
            70,
            999,
            QcFlag.PROBABLY_BAD_VALUE,
        ),  # 83.25701-(250+30) = -197, < -70*2 (3*sigma), >= -70*3
        (
            "DOXY_CTD",
            8.2,
            0.2,
            ["DOXY_BTL"],
            [8.3],
            [0.2],
            [
                [
                    "DOXY_BTL",
                ]
            ],
            0.3,
            None,
            QcFlag.GOOD_VALUE,
        ),  # 8.2-8.3 = -0.1, >= -2*0.28, <= 2*0.28
        (
            "DOXY_CTD",
            8.2,
            0.2,
            ["DOXY_BTL"],
            [8.8],
            [0.2],
            [
                [
                    "DOXY_BTL",
                ]
            ],
            0.3,
            None,
            QcFlag.PROBABLY_BAD_VALUE,
        ),  # 8.2-8.8 = -0.6, < -0.28*2, >= -0.28*3
        (
            "DOXY_CTD",
            8.8,
            0.2,
            ["DOXY_BTL"],
            [8.2],
            [0.2],
            [
                [
                    "DOXY_BTL",
                ]
            ],
            0.3,
            None,
            QcFlag.PROBABLY_BAD_VALUE,
        ),  # 8.8-8.2 = 0.6, > 0.28*2, <= 0.28*3
        (
            "DOXY_CTD",
            9.8,
            0.2,
            ["DOXY_BTL"],
            [8.2],
            [0.2],
            [
                [
                    "DOXY_BTL",
                ]
            ],
            0.3,
            None,
            QcFlag.BAD_VALUE,
        ),  # 8.8-8.2 = 1.6, > 0.28*3
        (
            "DOXY_CTD",
            9.8,
            0.2,
            ["DOXY_BTL"],
            [None],
            [None],
            [
                [
                    "DOXY_BTL",
                ]
            ],
            0.3,
            None,
            QcFlag.NO_QUALITY_CONTROL,
        ),  # 8.8-8.2 = 1.6, > 0.28*3
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
