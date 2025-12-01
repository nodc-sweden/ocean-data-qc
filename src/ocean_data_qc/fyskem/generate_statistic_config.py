from pathlib import Path

import pandas as pd
from jinja2 import Template

# Set up Jinja2 template
yaml_template_config = """
{%- for param_name, param_data in config_data.items() %}
{{ param_name }}:
    !!python/object:ocean_data_qc.fyskem.qc_checks.StatisticCheck
    sea_areas:
    {%- for sea_area, depth_list in param_data["sea_areas"].items() %}
      {{ sea_area }}:
      {%- for depth_entry in depth_list %}
      - min_depth: {{ depth_entry.min_depth }}
        max_depth: {{ depth_entry.max_depth }}
        months:
        {%- for month, month_config in depth_entry.months.items() %}
          '{{ month }}':
              min_range_value: {{ month_config.min_range_value }}
              max_range_value: {{ month_config.max_range_value }}
        {%- endfor %}
      {%- endfor %}
    {%- endfor %}
{%- endfor %}
"""

yaml_template_config_new = """
{%- for param_name, param_data in config_data.items() %}
{{ param_name }}:
    !!python/object:ocean_data_qc.fyskem.qc_checks.StatisticCheck
    filepath: "configs/statistic_check_data/{{ param_data["file_name"] }}"
{%- endfor %}
"""


def create_config_from_directory(data_directory):
    # Initialize config dictionary
    config_data = {}

    # Define file directory
    data_dir = Path(data_directory)

    # Loop over all CSV files
    for file_path in data_dir.glob("*.csv"):
        sea_area = file_path.stem  # Extract sea area from filename
        df = pd.read_csv(file_path, sep="\t", encoding="utf8")

        # Extract unique parameter names
        param_names = {col.split(":")[0] for col in df.columns if ":" in col}

        for param_name in param_names:
            mean_col = f"{param_name}:mean"
            std_col = f"{param_name}:std"
            count_col = f"{param_name}:count"
            min_col = f"{param_name}:min"
            max_col = f"{param_name}:max"

            # Initialize parameter config
            if param_name not in config_data:
                config_data[param_name] = {"sea_areas": {}}

            # Initialize sea area
            if sea_area not in config_data[param_name]["sea_areas"]:
                config_data[param_name]["sea_areas"][sea_area] = []

            # Group data by depth first
            for depth_interval, depth_group in df.groupby("depth_interval"):
                min_depth, max_depth = depth_interval.split("_")
                depth_entry = {
                    "min_depth": min_depth,
                    "max_depth": max_depth,
                    "months": {},
                }
                # Now loop over months within each depth
                for month, month_group in depth_group.groupby("month"):
                    month_str = str(int(month)).zfill(
                        2
                    )  # Ensure months are formatted as '01', '02', etc.

                    if (
                        mean_col not in month_group
                        or std_col not in month_group
                        or count_col not in month_group
                    ):
                        continue  # Skip if required columns are missing

                    mean_value = month_group[mean_col].values[0]
                    std_value = month_group[std_col].values[0]
                    count_value = month_group[count_col].values[0]
                    min_value = month_group[min_col].values[0]
                    max_value = month_group[max_col].values[0]

                    if pd.isna(mean_value) or pd.isna(std_value) or count_value <= 10:
                        continue  # Skip invalid values

                    min_range_value = float(min_value) - 2 * std_value
                    max_range_value = float(max_value) + 2 * std_value
                    # min_range_value = float(min_value) * 0.8  # decrease by 20%
                    # max_range_value = float(max_value) * 1.2  # increase by 20%

                    # special case for H2S, AMON, din and SIO3-SI where
                    # concentrations show a strong increasing trend
                    if (
                        sea_area
                        in [
                            "Eastern Gotland Basin",
                            "Western Gotland Basin",
                            "Northern Baltic Proper",
                        ]
                        and min_depth > 60
                        and param_name in ["H2S", "AMON", "din", "SIO3-SI"]
                    ):
                        min_range_value = float(min_value) - 3 * std_value
                        max_range_value = float(max_value) + 3 * std_value

                    # Add month config to the depth entry
                    depth_entry["months"][month_str] = {
                        "min_range_value": round(min_range_value, 2),
                        "max_range_value": round(max_range_value, 2),
                    }

                # Only add depth entry if it has valid months
                if depth_entry["months"]:
                    config_data[param_name]["sea_areas"][sea_area].append(depth_entry)
            # If not depth entries were added remove the key entirely for this param
            if len(config_data[param_name]["sea_areas"][sea_area]) == 0:
                config_data[param_name]["sea_areas"].pop(sea_area, None)

    return config_data


# To write the config to a YAML file
def write_yaml(config, output_file, yaml_template=yaml_template_config):
    # Render the YAML using Jinja2
    template = Template(yaml_template)
    yaml_output = template.render(config_data=config)

    # Save to file
    with open(output_file, "w", encoding="utf8") as yaml_file:
        yaml_file.write(yaml_output)

    print("YAML configuration successfully generated!")


def generate_statistic_parameter_files(data_directory, output_directory):
    """
    read file with statistics and create one file with statistics for each parameter
    Add columns to use for checks (min_range_value and max_range_value)
    """
    # Define file directory
    data_dir = Path(data_directory)
    output_dir = Path(output_directory)
    output_dir.mkdir(exist_ok=True)  # Ensure output directory exists

    # Dictionary to collect data per parameter
    parameter_data = {}

    # Loop over all CSV files
    for file_path in data_dir.glob("*.csv"):
        sea_basin = file_path.stem  # Extract sea basin name from filename
        print(sea_basin)
        df = pd.read_csv(file_path, sep="\t", encoding="utf-8")

        # Extract unique parameter names
        param_names = {col.split(":")[0] for col in df.columns if ":" in col}
        descriptive_cols = [col for col in df.columns if ":" not in col]

        for param in param_names:
            # Select only relevant columns for this parameter
            parameter_cols = [col for col in df.columns if col.startswith(param + ":")]
            if not parameter_cols:
                continue  # Skip if no relevant columns

            # Rename columns to remove parameter prefix (e.g., "TEMP:mean" → "mean")
            param_df = df[descriptive_cols + parameter_cols].copy()
            param_df.dropna(subset=parameter_cols, how="all", inplace=True)
            if param_df.empty:
                continue
            param_df.rename(
                columns=lambda x: x.split(":")[-1] if ":" in x else x, inplace=True
            )
            # Add sea basin column
            # param_df.insert(0, "sea_basin", sea_basin)
            # Collect data for this parameter
            if param not in parameter_data:
                parameter_data[param] = []
            parameter_data[param].append(param_df)
    config_data = {}

    # Save each parameter's collected data into a separate file
    for param, dataframes in parameter_data.items():
        param_df = pd.concat(dataframes, ignore_index=True)
        # Split the "depth_interval" column into "min_depth" and "max_depth"
        param_df[["min_depth", "max_depth"]] = param_df["depth_interval"].str.split(
            "_", expand=True
        )

        # description of the limits
        # upper limit for bad data: max + (75th percentile-median)
        # lower limit for bad data: min - (median - 25th percentile)
        # upper limit for correctable data: 99th percentile to upper limit for bad data
        # lower limit for bad data: 1st percentile to lower limit for bad data
        iqr_low = param_df["median"] - param_df["25p"]
        iqr_high = param_df["75p"] - param_df["median"]
        param_df["flag1_lower"] = round(param_df["1p"], 2)  # good down 1 percentile
        param_df["flag1_upper"] = round(param_df["99p"], 2)  # good up 99 percentile
        param_df["flag2_lower"] = round(param_df["1p"], 2)
        param_df["flag2_upper"] = round(param_df["99p"], 2)
        # correctable between 1 percentile and min - iqr_low,
        # all BELOW min will be flag 4 (bad)
        param_df["flag3_lower"] = round(param_df["min"] - iqr_low, 2)
        # correctable between 99-1 percentile and max + iqr_high,
        # all ABOVE will be flag 4 (bad)
        param_df["flag3_upper"] = round(param_df["max"] + iqr_high, 2)
        # Convert min_depth and max_depth to numeric (since split gives strings)
        param_df["min_depth"] = pd.to_numeric(param_df["min_depth"])
        param_df["max_depth"] = pd.to_numeric(param_df["max_depth"])

        # default use std *2 to define allowed max and min range
        # so far we only have good or bad in the test, not probably good
        param_df["min_range_value"] = round(param_df["min"] - param_df["std"] * 2, 2)
        param_df["max_range_value"] = round(param_df["max"] + param_df["std"] * 2, 2)

        # Special case: override with std * 3 where all conditions are met.
        # Applies to Baltic Proper below halocline (> 60 m)
        special_params = ["H2S", "AMON", "din", "SIO3-SI"]

        if param in special_params:
            special_basins = [
                "Eastern Gotland Basin",
                "Western Gotland Basin",
                "Northern Baltic Proper",
            ]
            mask = (param_df["sea_basin"].isin(special_basins)) & (
                param_df["depth"] >= 60
            )
            param_df.loc[mask, "min_range_value"] = round(
                param_df.loc[mask, "min"] - param_df.loc[mask, "std"] * 3, 2
            )
            param_df.loc[mask, "max_range_value"] = round(
                param_df.loc[mask, "max"] + param_df.loc[mask, "std"] * 3, 2
            )

        # Handle low temperatures and conc < 0
        flag_cols = param_df.filter(like="flag").columns
        if "TEMP" in param.upper():
            param_df["min_range_value"] = param_df["min_range_value"].clip(lower=-2)
            param_df[flag_cols] = param_df[flag_cols].clip(lower=-2)
        elif "neg" not in param:
            param_df["min_range_value"] = param_df["min_range_value"].clip(lower=0)
            param_df[flag_cols] = param_df[flag_cols].clip(lower=0)

        param_file = output_dir / f"{param}.txt"
        param_df.to_csv(param_file, sep="\t", index=False, encoding="utf8")
        config_data[param] = {"file_name": f"{param}.txt"}
        print(f"Saved: {param_file}")

    return config_data


# Running the code
if __name__ == "__main__":
    # Change this to the actual path
    directory_path = "../nodc-statistics/src/nodc_statistics/data/statistics_1990-2023"
    output_directory = "src/ocean_data_qc/fyskem/configs/statistic_check_data"
    qc_config = generate_statistic_parameter_files(
        data_directory=directory_path, output_directory=output_directory
    )
    write_yaml(
        qc_config,
        "src/ocean_data_qc/fyskem/configs/statistic_check.yaml",
        yaml_template=yaml_template_config_new,
    )
