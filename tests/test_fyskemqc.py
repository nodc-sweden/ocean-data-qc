import pytest
from fyskemqc.fyskemqc import FysKemQc
from fyskemqc.qc_flag import QcFlag
from fyskemqc.qc_flag_tuple import QcField
from setup_methods import generate_data_frame_of_length


@pytest.mark.parametrize("given_number_of_rows", (0, 42, 99))
def test_fyskemqc_length(given_number_of_rows):
    # Given data with given number of rows
    given_data = generate_data_frame_of_length(given_number_of_rows)
    assert len(given_data) == given_number_of_rows

    # When adding the data to FysKemQc object
    fyskemqc = FysKemQc(given_data)

    # Then it exposes the length of the data
    assert len(fyskemqc) == given_number_of_rows


def test_run_checks_for_parameters():
    # Given data
    given_data = generate_data_frame_of_length(5)

    # Given FysKemQc object
    given_fyskemeqc = FysKemQc(given_data)

    # And no automatic CQ has been performed
    assert len(given_fyskemeqc)
    assert all(
        qc_flag == QcFlag.NO_QC_PERFORMED
        for parameter in given_fyskemeqc.parameters
        for qc_flag in parameter.qc.automatic
    )

    # When running automatic QC
    given_fyskemeqc.run_automatic_qc()

    parameter_1 = given_fyskemeqc[0]
    parameter_2 = given_fyskemeqc[1]
    parameter_3 = given_fyskemeqc[2]
    parameter_4 = given_fyskemeqc[3]
    parameter_5 = given_fyskemeqc[4]

    # Then RangeCheck QC flags are changed
    assert parameter_1.qc.automatic[QcField.RangeCheck] != QcFlag.NO_QC_PERFORMED
    assert parameter_2.qc.automatic[QcField.RangeCheck] != QcFlag.NO_QC_PERFORMED
    assert parameter_3.qc.automatic[QcField.RangeCheck] != QcFlag.NO_QC_PERFORMED
    assert parameter_4.qc.automatic[QcField.RangeCheck] != QcFlag.NO_QC_PERFORMED
    assert parameter_5.qc.automatic[QcField.RangeCheck] != QcFlag.NO_QC_PERFORMED
