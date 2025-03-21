from dataclasses import dataclass
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
class StatisticCheck:
    """ "Holds the statistical threshold configuration for each parameter."""

    sea_areas: Dict[str, List[Dict]]

    def __getitem__(self, sea_area: str) -> List[Dict]:
        """Allows accessing sea areas like a dictionary (e.g., `config['Kattegat']`)."""
        return self.sea_areas.get(sea_area, [])

    def get_thresholds(
        self, sea_area: str, depth: float, month: int
    ) -> Optional[Dict[str, float]]:
        """Retrieves min and max thresholds for the given sea_area, depth, and month."""
        month_str = f"{int(month):02d}"  # Ensure two-digit month format

        # Find matching depth range
        for depth_range in self.sea_areas.get(sea_area, []):
            if depth_range["min_depth"] <= depth <= depth_range["max_depth"]:
                if month_str in depth_range["months"]:
                    min_range = depth_range["months"][month_str]["min_range_value"]
                    max_range = depth_range["months"][month_str]["max_range_value"]
                    return (min_range, max_range)
        return np.nan, np.nan
