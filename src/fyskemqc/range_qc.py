import numpy as np

from fyskemqc.parameter import Parameter
from fyskemqc.qc_checks import RangeCheck
from fyskemqc.qc_flag import QcFlag


class RangeQc:
    def __init__(self, configuration: RangeCheck):
        self._configuration = configuration

    def check(self, parameter: Parameter):
        qc_flag = QcFlag.NO_QC_PERFORMED
        if parameter.value in (None, np.nan):
            qc_flag = QcFlag.MISSING_VALUE
        elif self._configuration:
            if (
                self._configuration.min_range_value
                <= parameter.value
                <= self._configuration.max_range_value
            ):
                qc_flag = QcFlag.GOOD_DATA
            else:
                qc_flag = QcFlag.BAD_DATA
        parameter.qc.automatic[0] = qc_flag
