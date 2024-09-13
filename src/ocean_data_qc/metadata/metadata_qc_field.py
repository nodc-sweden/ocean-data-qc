import enum


class MetadataQcField(enum.Enum):
    """All metadata QC categories"""

    Wadep = 0
    DateAndTime = 1
    Position = 2
    CommonValues = 3
