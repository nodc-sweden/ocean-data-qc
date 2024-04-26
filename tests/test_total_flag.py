import pytest
from fyskemqc.qc_flag import QcFlag
from fyskemqc.qc_flags import QcFlags


@pytest.mark.parametrize(
    "given_flag_string",
    (
        "0_000_1",
        "1_345_2",
        "2_456_3",
        "3_567_4",
        "4_678_5",
        "5_789_6",
        "6_890_7",
        "7_901_8",
        "8_012_9",
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
        ("0_0_0", QcFlag.NO_QC_PERFORMED),
        ("1_0_0", QcFlag.GOOD_DATA),
        ("0_1_0", QcFlag.GOOD_DATA),
        ("2_0_0", QcFlag.PROBABLY_GOOD_DATA),
        ("0_78_0", QcFlag.INTERPOLATED_VALUE),
        ("2_3_0", QcFlag.BAD_DATA_CORRECTABLE),
        ("4_3_0", QcFlag.BAD_DATA),
        ("1_111511_0", QcFlag.VALUE_CHANGED),
        ("9_5678_0", QcFlag.MISSING_VALUE),
        ("7_56_0", QcFlag.NOMINAL_VALUE),
        ("3_561_0", QcFlag.BELOW_DETECTION),
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
