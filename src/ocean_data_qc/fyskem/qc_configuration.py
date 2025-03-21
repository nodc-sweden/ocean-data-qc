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
