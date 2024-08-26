from dataclasses import dataclass


@dataclass
class DetectionLimitCheck:
    limit: float


@dataclass
class RangeCheck:
    min_range_value: float
    max_range_value: float
