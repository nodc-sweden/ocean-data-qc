from enum import IntEnum

from ocean_data_qc import errors
from ocean_data_qc.fyskem.qc_flag import QcFlag


class QcField(IntEnum):
    """
    Handles:
        - Flag positions in the QcFlagTuple
        - The order tests are performed
    The qc-classes have the suffix Qc and the config classes the suffix Check
    """

    QuantificationLimit = 0
    Range = 1
    Statistic = 2
    RepeatedValue = 3
    Stability = 4
    Gradient = 5
    Spike = 6
    Consistency = 7
    H2s = 8
    Dependency = 9


# class QcFlagTuple:
#     """A tuple of QcFlags where elements can be assigned
#
#     To preserve the order of QcFlags,
#     this collection behaves like a tuple in all ways but
#     one: Elements can be assigned without the need to create a new object.
#     """
#
#     def __init__(self, *args, **kwargs):
#         inner_tuple = tuple(*args, **kwargs)
#         self._validate_new_elements(inner_tuple)
#         self._inner_tuple = inner_tuple
#
#     def _validate_new_elements(self, inner_tuple):
#         """All elements must be integers or a subclass thereof"""
#         if any(not isinstance(element, int) for element in inner_tuple):
#             raise errors.QcFlagTupleError(
#                 f"All values must be of type int. Values: {inner_tuple}"
#             )
#
#     def __getitem__(self, item):
#         return self._inner_tuple.__getitem__(item)
#
#     def __setitem__(self, index, value):
#         tuple_as_list = list(self._inner_tuple)
#
#         # Since order in the tuple has specific meanings,
#         # it is always possible to assign
#         # at a certain index. If the tuple is smaller than the index,
#         # it grows dynamically and fills any new elements with
#         # 'NO_CQ_PERFORMED'.
#         if index >= len(tuple_as_list):
#             elements_to_add = index - len(tuple_as_list) + 1
#             new_elements = [QcFlag.NO_QUALITY_CONTROL] * elements_to_add
#             new_elements[-1] = value
#             tuple_as_list += new_elements
#         tuple_as_list[index] = value
#         self._validate_new_elements(tuple_as_list)
#         self._inner_tuple = tuple_as_list
#
#     def __len__(self):
#         return len(self._inner_tuple)
#
#     def __eq__(self, other):
#         return self._inner_tuple == other
#
#     def __getattr__(self, name):
#         return getattr(self._inner_tuple, name)
#
#     def __iter__(self):
#         return iter(self._inner_tuple)
#
#     def __str__(self):
#         return "".join(str(v.value) for v in self._inner_tuple)
#
#     def __repr__(self):
#         return f"{__class__.__name__}{self._inner_tuple!s}"


class QcFlagTuple:
    """
    A tuple of QcFlags where elements can be assigned.

    Behaves like a tuple, but elements are QcFlag enums instead of ints.
    Supports:
        - Assignment at any index (tuple grows dynamically)
        - Validation of elements
        - Tuple-like methods (getitem, iter, len, eq, str, repr, getattr)
    """

    def __init__(self, *args, **kwargs):
        inner_tuple = tuple(*args, **kwargs)
        self._validate_new_elements(inner_tuple)
        self._inner_tuple = tuple(self._convert(v) for v in inner_tuple)

    def _convert(self, value):
        if isinstance(value, QcFlag):
            return value
        if isinstance(value, int) or isinstance(value, str):
            try:
                return QcFlag.parse(str(value))
            except Exception as e:
                raise errors.QcFlagTupleError(f"Invalid QC flag value: {value!r}") from e
        raise errors.QcFlagTupleError(f"Invalid element type: {value!r}")

    def _validate_new_elements(self, seq):
        for v in seq:
            try:
                self._convert(v)
            except errors.QcFlagTupleError as e:
                raise errors.QcFlagTupleError(
                    f"Validation failed for value: {v!r}"
                ) from e

    def __getitem__(self, item):
        return self._inner_tuple[item]

    def __setitem__(self, index, value):
        lst = list(self._inner_tuple)

        if index >= len(lst):
            extension = [QcFlag.NO_QUALITY_CONTROL] * (index - len(lst) + 1)
            lst.extend(extension)

        lst[index] = self._convert(value)

        self._validate_new_elements(lst)

        self._inner_tuple = tuple(lst)

    def __iter__(self):
        return iter(self._inner_tuple)

    def __len__(self):
        return len(self._inner_tuple)

    def __eq__(self, other):
        return self._inner_tuple == other

    def __getattr__(self, name):
        return getattr(self._inner_tuple, name)

    def __str__(self):
        return "".join(flag.value for flag in self._inner_tuple)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._inner_tuple!r})"
