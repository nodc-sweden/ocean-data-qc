import pytest
from fyskemqc.qc_flag import QcFlag


def test_sort_flags_using_key_function():
    # Given all available QC flag values
    given_qc_flags = list(QcFlag)

    # When sorting them using the provided key function
    sorted_list = sorted(given_qc_flags, key=QcFlag.key_function)

    # Then the order is as expected
    expected_order = [
        QcFlag.NO_QC_PERFORMED,
        QcFlag.BAD_DATA,
        QcFlag.MISSING_VALUE,
        QcFlag.INTERPOLATED_VALUE,
        QcFlag.NOMINAL_VALUE,
        QcFlag.BELOW_DETECTION,
        QcFlag.VALUE_CHANGED,
        QcFlag.BAD_DATA_CORRECTABLE,
        QcFlag.PROBABLY_GOOD_DATA,
        QcFlag.GOOD_DATA,
    ]

    assert sorted_list == expected_order


@pytest.mark.parametrize(
    "lesser_flag, greater_flag",
    (
        (QcFlag.NO_QC_PERFORMED, QcFlag.GOOD_DATA),
        (QcFlag.BAD_DATA, QcFlag.PROBABLY_GOOD_DATA),
        (QcFlag.MISSING_VALUE, QcFlag.BAD_DATA_CORRECTABLE),
        (QcFlag.INTERPOLATED_VALUE, QcFlag.VALUE_CHANGED),
        (QcFlag.NOMINAL_VALUE, QcFlag.BELOW_DETECTION),
    ),
)
def test_get_max_and_min_qc_flag_using_key_function(lesser_flag, greater_flag):
    assert max(lesser_flag, greater_flag, key=QcFlag.key_function) == greater_flag
    assert min(lesser_flag, greater_flag, key=QcFlag.key_function) == lesser_flag
