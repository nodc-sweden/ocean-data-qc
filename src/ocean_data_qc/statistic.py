import datetime
import functools
from pathlib import Path

import numpy as np
import polars as pl

# Path setup
STATISTICS_DIR = Path(__file__).parent / "fyskem" / "configs" / "statistic_check_data"
STATISTIC_FILES = {path.stem: path for path in STATISTICS_DIR.glob("*") if path.is_file()}


@functools.cache
def nan_float(value: str) -> float:
    try:
        return float(value)
    except ValueError:
        return np.nan


@functools.cache
def get_profile_statistics_for_parameter_and_sea_basin(
    parameter: str,
    sea_basin: str,
    point_in_time: datetime.datetime,
    statistics: tuple[str, ...] = ("median", "25p", "75p"),
) -> dict:
    statistic_path = STATISTIC_FILES.get(parameter)
    if not statistic_path:
        print(f"No statistic for {sea_basin} {parameter}")
        return _empty_result(statistics)

    try:
        df = pl.read_csv(statistic_path, separator="\t", try_parse_dates=True)
    except Exception as e:
        print(f"Failed to read {statistic_path}: {e}")
        return _empty_result(statistics)

    try:
        filtered_df = df.filter(
            (pl.col("month").cast(pl.Int64) == point_in_time.month)
            & (pl.col("sea_basin") == sea_basin)
        )
    except pl.ColumnNotFoundError:
        print(f"Missing expected columns in {parameter}")
        return _empty_result(statistics)

    if filtered_df.is_empty():
        return _empty_result(statistics)

    # Ensure depth is cast correctly
    filtered_df = filtered_df.with_columns([pl.col("depth").cast(pl.Float64)])

    output = {"depth": filtered_df["depth"].to_list()}

    for stat in statistics:
        if stat not in filtered_df.columns:
            print(f"Warning: '{stat}' not found in data for {parameter}, skipping.")
            output[stat] = [np.nan]
            continue

        # Cast to float, filter NaNs
        column = filtered_df[stat].cast(pl.Float64)
        valid = column.filter(~column.is_nan())

        output[stat] = valid.to_list() if not valid.is_empty() else [np.nan]

    return output


def _empty_result(statistics: tuple[str, ...]) -> dict:
    return {stat: [np.nan] for stat in ("depth", *statistics)}


# Example usage
if __name__ == "__main__":
    # Default stats (median, low_iqr, high_iqr)
    result = get_profile_statistics_for_parameter_and_sea_basin(
        "TEMP_CTD", "Kattegat", datetime.datetime(2024, 5, 16)
    )
    print("Default statistics:", result)

    # Custom stat list (must pass as a tuple!)
    custom_result = get_profile_statistics_for_parameter_and_sea_basin(
        "TEMP_CTD", "Kattegat", datetime.datetime(2024, 5, 16), statistics=("mean", "std")
    )
    print("Custom statistics:", custom_result)
