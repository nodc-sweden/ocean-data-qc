import enum

from ocean_data_qc import errors
from ocean_data_qc.fyskem.qc_flag import QcFlag


class QcField(enum.IntEnum):
    """Flag positions in the QcFlagTuple"""

    DetectionLimitCheck = 0
    RangeCheck = 1
    ConsistencyCheck = 2
    H2sCheck = 3
    IncreaseDecreaseCheck = 4


class QcFlagTuple:
    """A tuple of QcFlags where elements can be assigned

    To preserve the order of QcFlags, this collection behaves like a tuple in all ways but
    one: Elements can be assigned without the need to create a new object.
    """

    def __init__(self, *args, **kwargs):
        inner_tuple = tuple(*args, **kwargs)
        self._validate_new_elements(inner_tuple)
        self._inner_tuple = inner_tuple

    def _validate_new_elements(self, inner_tuple):
        """All elements must be integers or a subclass thereof"""
        if any(not isinstance(element, int) for element in inner_tuple):
            raise errors.QcFlagTupleError(
                f"All values must be of type int. Values: {inner_tuple}"
            )

    def __getitem__(self, item):
        return self._inner_tuple.__getitem__(item)

    def __setitem__(self, index, value):
        tuple_as_list = list(self._inner_tuple)

        # Since order in the tuple has specific meanings, it is always possible to assign
        # at a certain index. If the tuple is smaller than the index, it grows dynamically
        # and fills any new elements with 'NO_CQ_PERFORMED'.
        if index >= len(tuple_as_list):
            elements_to_add = index - len(tuple_as_list) + 1
            new_elements = [QcFlag.NO_QC_PERFORMED] * elements_to_add
            new_elements[-1] = value
            tuple_as_list += new_elements
        tuple_as_list[index] = value
        self._validate_new_elements(tuple_as_list)
        self._inner_tuple = tuple_as_list

    def __len__(self):
        return len(self._inner_tuple)

    def __eq__(self, other):
        return self._inner_tuple == other

    def __getattr__(self, name):
        return getattr(self._inner_tuple, name)

    def __iter__(self):
        return iter(self._inner_tuple)

    def __str__(self):
        return "".join(str(v.value) for v in self._inner_tuple)

    def __repr__(self):
        return f"{__class__.__name__}{str(self._inner_tuple)}"
