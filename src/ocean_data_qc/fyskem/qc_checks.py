import math
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import polars as pl


@dataclass
class DetectionLimitCheck:
    limit: float


@dataclass
class RangeCheck:
    min_range_value: float
    max_range_value: float


@dataclass
class RepeatedValueCheck:
    repeated_value: int


@dataclass
class StabilityCheck:
    bad_decrease: float
    probably_bad_decrease: float
    probably_good_decrease: float


@dataclass
class GradientCheck:
    allowed_decrease: float
    allowed_increase: float


@dataclass
class SpikeCheck:
    threshold_high: float
    threshold_low: float
    rate_of_change: float


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
class DependencyCheck:
    parameter_list: list


@dataclass
class StatisticCheck:
    """Holds the statistical threshold configuration for each parameter."""

    filepath: str  # Single file containing statistics for all sea areas
    _df: Optional[pl.DataFrame] = field(init=False, repr=False, default=None)

    @property
    def data(self) -> pl.DataFrame:
        """Lazy load the Polars DataFrame only when accessed."""
        if self._df is None:
            relative_filepath = Path(__file__).parent / self.filepath
            self._df = pl.read_csv(relative_filepath, separator="\t", encoding="utf8")
        return self._df

    def get_thresholds(self, sea_basin: str, depth: float, month: int) -> dict:
        """
        Retrieves thresholds as a dictionary.
        Returns NaNs if no matching configuration is found.
        """
        # Select columns relevant for thresholds
        threshold_cols = [
            "min_range_value",
            "max_range_value",
            "flag1_lower",
            "flag1_upper",
            "flag2_lower",
            "flag2_upper",
            "flag3_lower",
            "flag3_upper",
        ]
        month_str = f"{int(month):02d}"  # keep format consistent

        match = self.data.filter(
            (pl.col("sea_basin") == sea_basin)
            & (pl.col("month") == int(month_str))
            & (pl.col("min_depth") <= depth)
            & (pl.col("max_depth") > depth)
        )

        if match.height == 0:
            # Return NaN for all expected fields
            return {col: math.nan for col in threshold_cols}

        if match.height > 1:
            warnings.warn(
                f"Multiple rows ({match.height}) matched for sea_basin={sea_basin}, "
                "depth={depth}, month={month}. Using the first match in {match}"
            )
        row = match.row(0, named=True)

        return {col: float(row[col]) for col in threshold_cols}
