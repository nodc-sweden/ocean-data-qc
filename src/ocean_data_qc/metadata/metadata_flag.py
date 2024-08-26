import enum


class MetadataFlag(enum.IntEnum):
    NO_QC_PERFORMED = 0
    GOOD_DATA = 1
    BAD_DATA = 2
