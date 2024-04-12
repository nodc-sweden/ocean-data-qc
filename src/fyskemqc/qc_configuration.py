import importlib.resources

import yaml

# from fyskemqc.parameter import Parameter
import fyskemqc.qc_checks  # noqa: F401


class QcConfiguration:
    def __init__(self, configuration: dict = None):
        if configuration:
            self._configuration = configuration
        else:
            configuration_file = (
                importlib.resources.files("fyskemqc.configs") / "checks.yaml"
            )
            self._configuration = yaml.load(
                configuration_file.read_text(), Loader=yaml.Loader
            )

    def get(self, parameter):
        if configuration := self._configuration.get(parameter.name):
            return configuration.get("global")
        return None

    @classmethod
    def from_dict(cls, data: dict):
        cq_configuration = cls()
        cq_configuration._configuration = data
        return cq_configuration


if __name__ == "__main__":
    QcConfiguration()
