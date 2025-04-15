import pytest

from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField, QcFlagTuple
from ocean_data_qc.fyskem.qc_flags import QcFlags


def test_qc_flags_has_three_sections():
    # When creating a QcFlags object
    qc_flags = QcFlags()

    # Then it has an incoming flag
    assert qc_flags.incoming == QcFlag.NO_QC_PERFORMED

    # And it has a tuple of automatic flags
    assert len(qc_flags.automatic)
    assert all(flag == QcFlag.NO_QC_PERFORMED for flag in qc_flags.automatic)

    # And it has a manual flag
    assert qc_flags.manual == QcFlag.NO_QC_PERFORMED


def test_automatic_qc_flags_are_always_qc_flag_tuples():
    # Given a QcFlags object
    given_qc_flags = QcFlags()

    # And the manual section is a QcFlagTuple
    assert isinstance(given_qc_flags.automatic, QcFlagTuple)

    # When setting a new value using a list
    given_qc_flags.automatic = [QcFlag.VALUE_CHANGED, QcFlag.BELOW_DETECTION]

    # Then the manual section is still a QcFlagTuple
    assert isinstance(given_qc_flags.automatic, QcFlagTuple)


def test_empty_flags_string_becomes_non_empty_qc_flags_object():
    # Given an empty string
    given_string = ""

    # When creating a QcFlags object from the string
    qc_flags = QcFlags.from_string(given_string)

    # Then the object has values
    assert qc_flags.incoming == QcFlag.NO_QC_PERFORMED
    assert len(qc_flags.automatic)
    assert all(flag == QcFlag.NO_QC_PERFORMED for flag in qc_flags.automatic)
    assert qc_flags.manual == QcFlag.NO_QC_PERFORMED


@pytest.mark.parametrize(
    "given_qc_flags", ("0_0_0_0", "1_23_4_4", "5_4321_0_4", "1_235678_9_9")
)  # noqa: E501
def test_qc_flags_roundtrip(given_qc_flags: str):
    # Given a qc flags string
    # When creating a QcFlags object
    qc_flags = QcFlags.from_string(given_qc_flags)

    # Then the initial string is preserved
    assert str(qc_flags) == given_qc_flags


@pytest.mark.parametrize(
    "given_qc_flags, given_field, expected_value",
    (
        ("3_2257111317_4_4", QcField.DetectionLimitCheck, QcFlag.PROBABLY_GOOD_DATA),
        ("5_8976543210_6_4", QcField.RangeCheck, QcFlag.INTERPOLATED_VALUE),
    ),
)
def test_get_automatic_qc_flag_by_position(given_qc_flags, given_field, expected_value):
    # Given a QcFlags object
    qc_flags = QcFlags.from_string(given_qc_flags)

    # When requesting specific field from the automatic QC flags
    value = qc_flags.automatic[given_field]

    # Then the expected value is retrieved
    assert value == expected_value
