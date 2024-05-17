from pathlib import Path

import yaml

# from fyskemqc.parameter import Parameter
import fyskemqc.qc_checks  # noqa: F401
from fyskemqc.parameter import Parameter


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

    def get(self, category: str, parameter: Parameter):
        if configuration := self._configuration.get(category, {}).get(parameter.name):
            return configuration.get("global")
        return None

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
