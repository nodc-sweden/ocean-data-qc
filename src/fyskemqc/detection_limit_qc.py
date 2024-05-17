import numpy as np

from fyskemqc.parameter import Parameter
from fyskemqc.qc_checks import DetectionLimitCheck
from fyskemqc.qc_flag import QcFlag
from fyskemqc.qc_flag_tuple import QcField


class DetectionLimitQc:
    def __init__(self, configuration: DetectionLimitCheck):
        self._configuration = configuration

    def check(self, parameter: Parameter):
        qc_flag = QcFlag.NO_QC_PERFORMED
        if parameter.value in (None, np.nan):
            qc_flag = QcFlag.MISSING_VALUE
        elif self._configuration:
            if parameter.value >= self._configuration.limit:
                qc_flag = QcFlag.GOOD_DATA
            else:
                qc_flag = QcFlag.BELOW_DETECTION

        parameter.qc.automatic[QcField.DetectionLimitCheck] = qc_flag
