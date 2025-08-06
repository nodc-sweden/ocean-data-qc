from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd


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
    good_lower: float
    good_upper: float
    max_lower: float
    max_upper: float


@dataclass
class H2sCheck:
    skip_flag: str


@dataclass
class IncreaseDecreaseCheck:
    allowed_decrease: float
    allowed_increase: float


@dataclass
class RepeatedValueCheck:
    repeated_value: int


@dataclass
class SpikeCheck:
    allowed_delta: float
    allowed_depths: list


@dataclass
class StatisticCheck:
    """Holds the statistical threshold configuration for each parameter."""

    filepath: str  # Single file containing statistics for all sea areas
    _df: Optional[pd.DataFrame] = field(init=False, repr=False, default=None)

    @property
    def data(self):
        """Lazy load the DataFrame only when accessed."""
        relative_filepath = Path(__file__).parent / self.filepath
        if self._df is None:
            self._df = pd.read_csv(relative_filepath, sep="\t", encoding="utf8")
        return self._df

    def get_thresholds(
        self, sea_basin: str, depth: float, month: int
    ) -> Tuple[float, float]:
        """Retrieves min and max thresholds using optimized filtering."""
        month_str = f"{int(month):02d}"  # Ensure two-digit month format

        # Filter data by sea_basin and month
        filtered = self.data.loc[
            (self.data["sea_basin"] == sea_basin) & (self.data["month"] == int(month_str))
        ]

        # Further filter by depth interval
        match = filtered.loc[
            (filtered["min_depth"] <= depth) & (filtered["max_depth"] >= depth)
        ]

        if match.empty:
            return (
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            )  # No matching data found

        # Extract min/max values (assuming one row matches)
        min_range = float(match["min_range_value"].values[0])
        max_range = float(match["max_range_value"].values[0])
        flag1_lower = float(match["flag1_lower"].values[0])
        flag1_upper = float(match["flag1_upper"].values[0])
        flag2_lower = float(match["flag2_lower"].values[0])
        flag2_upper = float(match["flag2_upper"].values[0])
        flag3_lower = float(match["flag3_lower"].values[0])
        flag3_upper = float(match["flag3_upper"].values[0])

        return (
            min_range,
            max_range,
            flag1_lower,
            flag1_upper,
            flag2_lower,
            flag2_upper,
            flag3_lower,
            flag3_upper,
        )
