from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np


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
    lower_deviation: float
    upper_deviation: float


@dataclass
class H2sCheck:
    skip_flag: str


@dataclass
class IncreaseDecreaseCheck:
    allowed_decrease: float
    allowed_increase: float


@dataclass
class MonthConfig:
    min_range_value: float
    max_range_value: float


@dataclass
class DepthRangeConfig:
    min_depth: float
    max_depth: float
    months: Dict[str, MonthConfig]


@dataclass
class StatisticCheck:
    """Holds the statistical threshold configuration for each parameter."""

    sea_areas: Dict[str, List[DepthRangeConfig]] = field(default_factory=dict)

    def __getitem__(self, sea_area: str) -> List[DepthRangeConfig]:
        """Allows accessing sea areas like a dictionary (e.g., `config['Kattegat']`)."""
        return self.sea_areas.get(sea_area, [])

    def get_thresholds(self, sea_basin: str, depth: float, month: int) -> Optional[tuple]:
        """
        Get the min/max threshold values based on sea_area, depth, and month.

        Returns:
            (min_range_value, max_range_value) if found, otherwise (nan, nan).
        """
        # something in the input args is missing
        if any(arg is None for arg in [sea_basin, depth, month]):
            return np.nan, np.nan
        if np.isnan(depth):
            return np.nan, np.nan

        # Step 1: Get the list of depth ranges for the given sea_area
        depth_ranges = self.sea_areas.get(sea_basin, [])
        if not depth_ranges:
            return np.nan, np.nan  # Sea area not found
        # Step 2: Find the matching depth range
        for depth_range in depth_ranges:
            if np.isnan(depth_range.min_depth) or np.isnan(depth_range.max_depth):
                return np.nan, np.nan  # missing depth range for sea area
            if depth_range.min_depth <= depth <= depth_range.max_depth:
                # Step 3: Find the matching month
                month_config = depth_range.months.get(month)
                if month_config:
                    return month_config.min_range_value, month_config.max_range_value

        # No match found
        return np.nan, np.nan
