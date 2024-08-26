import pytest
from ocean_data_qc.fyskem.qc_flag import QcFlag


def test_sort_flags_using_key_function():
    # Given all available QC flag values
    given_qc_flags = list(QcFlag)

    # When sorting them using the provided key function
    sorted_list = sorted(given_qc_flags, key=QcFlag.key_function)

    # Then the order is as expected
    expected_order = [
        QcFlag.BAD_DATA,
        QcFlag.MISSING_VALUE,
        QcFlag.INTERPOLATED_VALUE,
        QcFlag.NOMINAL_VALUE,
        QcFlag.BELOW_DETECTION,
        QcFlag.VALUE_CHANGED,
        QcFlag.BAD_DATA_CORRECTABLE,
        QcFlag.PROBABLY_GOOD_DATA,
        QcFlag.GOOD_DATA,
        QcFlag.NO_QC_PERFORMED,
    ]

    assert sorted_list == expected_order


@pytest.mark.parametrize(
    "lesser_flag, greater_flag",
    (
        (QcFlag.GOOD_DATA, QcFlag.NO_QC_PERFORMED),
        (QcFlag.BAD_DATA, QcFlag.PROBABLY_GOOD_DATA),
        (QcFlag.MISSING_VALUE, QcFlag.BAD_DATA_CORRECTABLE),
        (QcFlag.INTERPOLATED_VALUE, QcFlag.VALUE_CHANGED),
        (QcFlag.NOMINAL_VALUE, QcFlag.BELOW_DETECTION),
    ),
)
def test_get_max_and_min_qc_flag_using_key_function(lesser_flag, greater_flag):
    assert max(lesser_flag, greater_flag, key=QcFlag.key_function) == greater_flag
    assert min(lesser_flag, greater_flag, key=QcFlag.key_function) == lesser_flag


@pytest.mark.parametrize(
    "given_input, expected_flag",
    (
        ("0", QcFlag.NO_QC_PERFORMED),
        ("1", QcFlag.GOOD_DATA),
        ("2", QcFlag.PROBABLY_GOOD_DATA),
        ("3", QcFlag.BAD_DATA_CORRECTABLE),
        ("4", QcFlag.BAD_DATA),
        ("5", QcFlag.VALUE_CHANGED),
        ("6", QcFlag.BELOW_DETECTION),
        ("7", QcFlag.NOMINAL_VALUE),
        ("8", QcFlag.INTERPOLATED_VALUE),
        ("9", QcFlag.MISSING_VALUE),
    ),
)
def test_parsing_method_can_handle_str(given_input, expected_flag):
    # Given an input that is str
    assert isinstance(given_input, str)

    # When parsing it as a QcFlag
    cq_flag = QcFlag.parse(given_input)

    # Then it is interpreted as the expected flag
    assert cq_flag == expected_flag


@pytest.mark.parametrize(
    "given_input, expected_flag",
    (
        (0, QcFlag.NO_QC_PERFORMED),
        (1, QcFlag.GOOD_DATA),
        (2, QcFlag.PROBABLY_GOOD_DATA),
        (3, QcFlag.BAD_DATA_CORRECTABLE),
        (4, QcFlag.BAD_DATA),
        (5, QcFlag.VALUE_CHANGED),
        (6, QcFlag.BELOW_DETECTION),
        (7, QcFlag.NOMINAL_VALUE),
        (8, QcFlag.INTERPOLATED_VALUE),
        (9, QcFlag.MISSING_VALUE),
    ),
)
def test_parsing_method_can_handle_int(given_input, expected_flag):
    # Given an input that is str
    assert isinstance(given_input, int)

    # When parsing it as a QcFlag
    cq_flag = QcFlag.parse(given_input)

    # Then it is interpreted as the expected flag
    assert cq_flag == expected_flag


def test_parsing_method_treats_empty_string_as_no_qc():
    # Given input is an empty string
    given_input = ""

    # When parsing it as a QcFlag
    cq_flag = QcFlag.parse(given_input)

    # Then it is interpreted as: No QC performed
    assert cq_flag == QcFlag.NO_QC_PERFORMED


def test_parsing_method_treats_none_as_no_qc():
    # Given input is None
    given_input = None

    # When parsing it as a QcFlag
    cq_flag = QcFlag.parse(given_input)

    # Then it is interpreted as: No QC performed
    assert cq_flag == QcFlag.NO_QC_PERFORMED
