import pytest

from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flags import QcFlags


@pytest.mark.parametrize(
    "given_flag_string",
    (
        "0_000_1_1",
        "1_345_2_4",
        "2_456_3_4",
        "3_567_4_4",
        "4_678_5_4",
        "5_789_6_6",
        "6_890_7_6",
        "7_901_8_7",
        "8_012_9_8",
    ),
)
def test_manual_qc_trumps_all_other_flags(given_flag_string):
    # Given QC flags
    given_qc_flags = QcFlags.from_string(given_flag_string)

    # Given manual QC has been performed
    assert given_qc_flags.manual != QcFlag.NO_QC_PERFORMED

    # When reading the total flag
    # Then the total flag is equal to the manual flag
    assert given_qc_flags.total == given_qc_flags.manual


@pytest.mark.parametrize(
    "given_flag_string, expected_total",
    (
        ("0_0_0_0", QcFlag.NO_QC_PERFORMED),
        ("1_0_0_1", QcFlag.GOOD_DATA),
        ("0_1_0_1", QcFlag.GOOD_DATA),
        ("2_0_0_2", QcFlag.PROBABLY_GOOD_DATA),
        ("0_78_0_0", QcFlag.INTERPOLATED_VALUE),
        ("2_3_0_3", QcFlag.BAD_DATA_CORRECTABLE),
        ("4_3_0_4", QcFlag.BAD_DATA),
        ("1_111511_0_5", QcFlag.VALUE_CHANGED),
        ("9_5678_0_9", QcFlag.MISSING_VALUE),
        ("7_56_0_7", QcFlag.NOMINAL_VALUE),
        ("3_561_0_6", QcFlag.BELOW_DETECTION),
    ),
)
def test_without_manual_the_total_flag_is_worst_from_incoming_and_automatic(
    given_flag_string, expected_total
):
    # Given QC flags
    given_qc_flags = QcFlags.from_string(given_flag_string)

    # Given manual QC is not performed
    assert given_qc_flags.manual == QcFlag.NO_QC_PERFORMED

    # When reading the total flag
    # Then the total flag is the lowest individual flag
    assert given_qc_flags.total == expected_total


@pytest.mark.parametrize(
    "given_flag_string",
    (
        "0_1_0_1",
        "0_01_0_1",
        "0_1234567890_0_4",
        "1_10234_0_4",
        "2_98706_0_6",
        "3_30456_0_4",
        "4_65403_0_4",
        "5_56078_0_6",
        "6_87065_0_6",
        "7_20345_0_4",
        "8_54320_0_4",
        "9_02936_0_9",
    ),
)
def test_no_qc_performed_should_always_be_chosen_last(given_flag_string):
    # Given QC flags
    given_qc_flags = QcFlags.from_string(given_flag_string)

    # Given "No QC performed" is present in either incoming or automatic.
    all_present_flags = {given_qc_flags.incoming} | set(given_qc_flags.automatic)
    assert QcFlag.NO_QC_PERFORMED in all_present_flags

    # Given there are other flags present
    assert all_present_flags - {QcFlag.NO_QC_PERFORMED}

    # When reading the total flag
    # Then the total flag is not "No QC performed"
    assert given_qc_flags.total != QcFlag.NO_QC_PERFORMED
