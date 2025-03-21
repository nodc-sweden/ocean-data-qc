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

                    min_range_value = mean_value - 2 * std_value
                    max_range_value = mean_value + 2 * std_value
                    min_range_value = float(min_value) * 0.8  # decrease by 20%
                    max_range_value = float(max_value) * 1.2  # increase by 20%

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


# Running the code
if __name__ == "__main__":
    # Change this to the actual path
    directory_path = "nodc-statistics/src/nodc_statistics/data/statistics"
    qc_config = create_config_from_directory(directory_path)
    write_yaml(qc_config, "src/ocean_data_qc/fyskem/configs/statistic_check.yaml")
