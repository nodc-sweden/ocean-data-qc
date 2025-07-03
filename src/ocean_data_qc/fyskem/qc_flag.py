import enum
from collections import defaultdict


class QcFlag(enum.IntEnum):
    NO_QC_PERFORMED = 0
    GOOD_DATA = 1
    PROBABLY_GOOD_DATA = 2
    BAD_DATA_CORRECTABLE = 3
    BAD_DATA = 4
    VALUE_CHANGED = 5
    BELOW_DETECTION = 6
    VALUE_IN_EXCESS = 7
    INTERPOLATED_VALUE = 8
    MISSING_VALUE = 9

    __PRIORITY = (4, 9, 8, 7, 6, 5, 3, 2, 1, 0)

    @classmethod
    def key_function(cls, value):
        """Key function for QcFlag

        Used for sorting, min and max.
        """
        return cls.__PRIORITY.index(value)

    def __str__(self):
        return self.name.replace("_", " ").capitalize().replace("qc", "QC")

    @classmethod
    def parse(cls, value):
        """A more liberal parser for the Enum

        The parser will accept str, int and None. None and the empty string will be
        interpreted as "NO_QC_PERFORMED".
        """
        if value in ("", None):
            return cls.NO_QC_PERFORMED
        return cls(int(value))


QC_FLAG_CSS_COLORS = defaultdict(
    lambda: "gray",
    {
        QcFlag.NO_QC_PERFORMED: "navy",
        QcFlag.PROBABLY_GOOD_DATA: "#9AC23D",
        QcFlag.BAD_DATA_CORRECTABLE: "orange",
        QcFlag.BAD_DATA: "red",
        QcFlag.GOOD_DATA: "#008000",
        QcFlag.BELOW_DETECTION: "pink",
        QcFlag.VALUE_IN_EXCESS: "pink",
    },
)
