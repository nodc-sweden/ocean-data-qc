import pytest

from ocean_data_qc import errors
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcFlagTuple


def test_qc_flag_tuple_behaves_like_a_tuple():
    # Given a QcFlagTuple
    original_tuple = (1, 5, 2, 4, 3)
    given_qc_flag_tuple = QcFlagTuple(original_tuple)

    # It has a size
    assert len(given_qc_flag_tuple) == 5

    # Values can be accessed
    assert given_qc_flag_tuple[0] == 1
    assert given_qc_flag_tuple[1] == 5
    assert given_qc_flag_tuple[2] == 2
    assert given_qc_flag_tuple[3] == 4
    assert given_qc_flag_tuple[4] == 3

    # Tuple methods works
    assert given_qc_flag_tuple.count(2) == 1
    assert given_qc_flag_tuple.index(4) == 3

    # Equality checks works
    assert given_qc_flag_tuple == given_qc_flag_tuple
    assert given_qc_flag_tuple == original_tuple
    assert given_qc_flag_tuple != original_tuple + (100,)

    # It can not be sorted in place
    with pytest.raises(AttributeError):
        given_qc_flag_tuple.sort()

    # But like any sequence, it can be reversed
    sorted_tuple = list(reversed(given_qc_flag_tuple))
    assert sorted_tuple == [3, 4, 2, 5, 1]

    # ...and sorted
    sorted_tuple = sorted(given_qc_flag_tuple)
    assert sorted_tuple == [1, 2, 3, 4, 5]

    # Values can not be deleted
    with pytest.raises(AttributeError):
        del given_qc_flag_tuple[2]


def test_qc_flag_tuple_differs_from_a_tuple():
    # Given a QcFlagTuple
    given_qc_flag_tuple = QcFlagTuple([1, 2, 3, 4])

    # And the object has an id
    object_id_before = id(given_qc_flag_tuple)

    # When setting new values for the elements
    given_qc_flag_tuple[0] = 5
    given_qc_flag_tuple[1] = 6
    given_qc_flag_tuple[2] = 7
    given_qc_flag_tuple[3] = 8

    # Then the values are changed
    assert given_qc_flag_tuple[0] == 5
    assert given_qc_flag_tuple[1] == 6
    assert given_qc_flag_tuple[2] == 7
    assert given_qc_flag_tuple[3] == 8

    # And it is still the same object (i.e. same id)
    assert object_id_before == id(given_qc_flag_tuple)


@pytest.mark.parametrize(
    "given_length, given_index", ((0, 0), (1, 1), (0, 10), (5, 9), (9, 10))
)
def test_when_setting_a_value_outside_a_qc_flag_tuple_the_tuple_grows(
    given_length, given_index
):
    # Given a QcFlagTuple of a given length
    given_qc_flag_tuple = QcFlagTuple((QcFlag.GOOD_DATA,) * given_length)
    assert len(given_qc_flag_tuple) == given_length

    # Given an index outside the given QcFlagTuple
    assert given_index >= len(given_qc_flag_tuple)

    # When adding a value to the given index
    given_qc_flag_tuple[given_index] = QcFlag.BAD_DATA

    # Then the QcFlagTuple grows
    expected_new_size = given_index + 1
    assert len(given_qc_flag_tuple) == expected_new_size

    # And values are added as needed
    new_indices = range(max(0, given_length), given_index)
    assert all(
        given_qc_flag_tuple[added_index] == QcFlag.NO_QC_PERFORMED
        for added_index in new_indices
    )


def test_initial_elements_must_be_integers():
    # Given a sequence that includes something else than integers
    given_sequence = [1, "2", 3]

    # When trying to create a QcFlagTuple
    # Then it raises
    with pytest.raises(errors.QcFlagTupleError):
        QcFlagTuple(given_sequence)


def test_added_elements_must_be_integers():
    # Given a QcFlagTuple
    given_qc_flag_tuple = QcFlagTuple()

    # Given a value that is not an integer
    given_value = "not and integer"

    # When trying to add the value to the QcFlagTuple
    # Then it raises
    with pytest.raises(errors.QcFlagTupleError):
        given_qc_flag_tuple[0] = given_value
