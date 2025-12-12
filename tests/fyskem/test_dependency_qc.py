import pytest

from ocean_data_qc.fyskem.dependency_qc import DependencyQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from tests.setup_methods import generate_data_frame, generate_dependency_configuration


@pytest.mark.parametrize(
    "given_parameter, given_value, given_parameter_quality_flag_long,"
    "given_parameter_list, given_list_values, given_list_quality_flag_long,"
    "expected_flag",
    (
        (
            "NTRA",
            7.8,
            "0_0000000000_0_0",
            ["NTRZ", "NTRI"],
            [7.9, 0.1],
            ["0_1113201000_0_0", "6_6110001000_0_0"],
            QcFlag.PROBABLY_BAD_VALUE,
        ),
        (
            "SALT_CTD",
            34.5,
            "0_0000000000_0_0",
            ["TEMP_CTD", "Derived in situ density CTD"],
            [10.3, 1025.2],
            ["0_1113200000_0_0", "0_0000400000_0_0"],
            QcFlag.BAD_VALUE,
        ),
        (
            "DOXY_CTD",
            8.2,
            "1_0000000000_0_1",
            [
                "TEMP_CTD",
                "SALT_CTD",
                "Derived in situ density CTD",
                "Derived oxygen saturation CTD",
            ],
            [10.2, 34.7, 1025.3, 97.8],
            ["1_1110000000_0_1", "1_0000100000_0_1", "1_0100000000_0_1"],
            QcFlag.GOOD_VALUE,
        ),
    ),
)
def test_dependency_check_using_override_configuration(
    given_parameter,
    given_value,
    given_parameter_quality_flag_long,
    given_parameter_list,
    given_list_values,
    given_list_quality_flag_long,
    expected_flag,
):
    # Given parameters with given values for a given depth and visit_key
    given_parameters = [given_parameter, *given_parameter_list]
    given_values = [given_value, *given_list_values]
    given_flags = [given_parameter_quality_flag_long, *given_list_quality_flag_long]
    given_depth = 20
    given_visit_key = "ABC123"

    rows = [
        {
            "parameter": p,
            "value": v,
            "quality_flag_long": f,
            "DEPH": given_depth,
            "visit_key": given_visit_key,
        }
        for p, v, f in zip(given_parameters, given_values, given_flags)
    ]

    given_data = generate_data_frame(rows)

    # Given a dependency_qc object has been initiated with an override configuration that
    # includes given parameter
    given_configuration = generate_dependency_configuration(
        given_parameter,
        given_parameter_list,
    )

    dependency_qc = DependencyQc(given_data)
    dependency_qc.expand_qc_columns()

    # When performing QC
    dependency_qc.check(given_parameter, given_configuration)

    # And finalizing data
    dependency_qc.collapse_qc_columns()
    given_data = dependency_qc._data

    # Then the automatic QC flags has at least as many positions
    # to include the field for the Dependency Check
    parameter_after = Parameter(given_data.row(0, named=True))
    assert len(parameter_after.qc.automatic) >= (QcField.Dependency + 1)

    # And the parameter is given the expected flag at the expected position
    assert parameter_after.qc.automatic[QcField.Dependency] == expected_flag
