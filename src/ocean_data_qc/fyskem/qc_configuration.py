from pathlib import Path

import yaml


class QcConfiguration:
    def __init__(self, configuration: dict = None):
        if configuration:
            self._configuration = configuration
        else:
            self._configuration = {}
            config_dir = Path(__file__).parent / "configs"
            for yaml_file in config_dir.glob("*.yaml"):
                category = yaml_file.stem
                self._configuration[category] = yaml.load(
                    yaml_file.read_text(), Loader=yaml.Loader
                )

            # Special case for StatisticCheck
            if "statistic_check" in self._configuration:
                self._configuration["statistic_check"] = self._parse_statistic_check(
                    self._configuration["statistic_check"]
                )

    def _parse_statistic_check(self, raw_config: dict) -> dict:
        """Parses StatisticCheck YAML into structured dataclasses."""
        config_dict = {}

        for parameter, parameter_config in raw_config.items():
            stat_check = parameter_config
            config_dict[parameter] = stat_check

        return config_dict

    def get(self, category: str, parameter: str):
        if category == "statistic_check":
            return self._configuration.get(category, {}).get(parameter)
        if configuration := self._configuration.get(category, {}).get(parameter):
            return configuration.get("global")
        return None

    def parameters(self, category: str):
        return self._configuration.get(category, {}).keys()

    @property
    def categories(self):
        return self._configuration.keys()

    @classmethod
    def from_dict(cls, data: dict):
        cq_configuration = cls()
        cq_configuration._configuration = data
        return cq_configuration


if __name__ == "__main__":
    QcConfiguration()
