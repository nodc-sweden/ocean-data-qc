import enum
from collections import defaultdict


class QcFlag(enum.Enum):
    NO_QUALITY_CONTROL = "0"
    GOOD_VALUE = "1"
    PROBABLY_GOOD_VALUE = "2"
    PROBABLY_BAD_VALUE = "3"
    BAD_VALUE = "4"
    CHANGED_VALUE = "5"
    VALUE_BELOW_DETECTION = "6"
    VALUE_IN_EXCESS = "7"
    INTERPOLATED_VALUE = "8"
    MISSING_VALUE = "9"
    VALUE_BELOW_LIMIT_OF_QUANTIFICATION = "Q"
    NOMINAL_VALUE = "B"
    VALUE_PHENOMENON_UNCERTAIN = "A"

    __PRIORITY = ("4", "9", "8", "7", "B", "A", "Q", "6", "5", "3", "2", "1", "0")

    # @classmethod
    # def key_function(cls, value):
    #     """Key function for QcFlag
    #
    #     Used for sorting, min and max.
    #     """
    #     return cls.__PRIORITY.index(value)

    @classmethod
    def key_function(cls, flag):
        """Return sort key for QcFlag."""
        return cls.__PRIORITY.index(flag.value)

    def __str__(self):
        return self.name.replace("_", " ").capitalize().replace("qc", "QC")

    #
    # @classmethod
    # def parse(cls, value):
    #     """A more liberal parser for the Enum
    #
    #     The parser will accept str, int and None. None and the empty string will be
    #     interpreted as "NO_QUALTIY_CONTROL".
    #     """
    #     if value in ("", None):
    #         return cls.NO_QUALITY_CONTROL
    #     return cls(int(value))

    @classmethod
    def parse(cls, value):
        """Liberal parser: accept str, int, None."""
        if value in ("", None):
            return cls.NO_QUALITY_CONTROL

        # If it is already a QcFlag
        if isinstance(value, cls):
            return value

        # Try exact match on the *string* enum value
        value_str = str(value)

        # Direct lookup by value
        for flag in cls:
            if flag.value == value_str:
                return flag

        raise ValueError(f"Invalid QC flag: {value!r}")


# QC_FLAG_CSS_COLORS = defaultdict(
#     lambda: "gray",
#     {
#         QcFlag.NO_QUALITY_CONTROL: "navy",
#         QcFlag.PROBABLY_GOOD_VALUE: "#9AC23D",
#         QcFlag.PROBABLY_BAD_VALUE: "orange",
#         QcFlag.BAD_VALUE: "red",
#         QcFlag.GOOD_VALUE: "#008000",
#         QcFlag.VALUE_BELOW_DETECTION: "pink",
#         QcFlag.VALUE_IN_EXCESS: "pink",
#     },
# )
QC_FLAG_CSS_COLORS = defaultdict(
    lambda: "gray",
    {
        QcFlag.NO_QUALITY_CONTROL: "navy",  # 0 blå
        QcFlag.GOOD_VALUE: "#008000",  # 1 grön
        QcFlag.PROBABLY_GOOD_VALUE: "#9AC23D",  # 2 ljusgrön
        QcFlag.PROBABLY_BAD_VALUE: "orange",  # 3 orange
        QcFlag.BAD_VALUE: "red",  # 4 röd
        QcFlag.CHANGED_VALUE: "#0066FF",  # 5 blå
        QcFlag.VALUE_BELOW_DETECTION: "pink",  # 6 rosa
        QcFlag.VALUE_IN_EXCESS: "pink",  # 7 rosa
        QcFlag.INTERPOLATED_VALUE: "#9370DB",  # 8 lila
        QcFlag.MISSING_VALUE: "gray",  # 9 grå
        QcFlag.VALUE_BELOW_LIMIT_OF_QUANTIFICATION: "#FFB6C1",  # Q ljusrosa
        QcFlag.NOMINAL_VALUE: "#1E90FF",  # B blå nyans
        QcFlag.VALUE_PHENOMENON_UNCERTAIN: "#8A2BE2",  # A blålila
    },
)
