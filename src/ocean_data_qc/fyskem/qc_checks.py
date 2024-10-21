from dataclasses import dataclass


@dataclass
class DetectionLimitCheck:
    limit: float


@dataclass
class RangeCheck:
    min_range_value: float
    max_range_value: float


@dataclass
class ConsistencyCheck:
    parameter_list: list


@dataclass
class H2sCheck:
    skip_flag: str
