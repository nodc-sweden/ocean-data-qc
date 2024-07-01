import pandas as pd

from fyskemqc.detection_limit_qc import DetectionLimitQc
from fyskemqc.parameter import Parameter
from fyskemqc.qc_configuration import QcConfiguration
from fyskemqc.range_qc import RangeQc

QC_CATEGORIES = {
    "range_check": RangeQc,
    "detection_limit_check": DetectionLimitQc,
}


class FysKemQc:
    def __init__(self, data: pd.DataFrame, stations: dict):
        """"""
        if "quality_flag_long" not in data:
            data["quality_flag_long"] = ""

        self._data = data
        self._stations = stations
        print('hej nu kommer stationsdictionary med')
        self._configuration = QcConfiguration()

    def __len__(self):
        return len(self._data)

    def __getitem__(self, index):
        series = self._data.iloc[index]
        return Parameter(series, index)

    @property
    def parameters(self):
        return {Parameter(series, index) for index, series in self._data.iterrows()}
    
    def loopa_series(self):
        for series, data in self._stations.items():
            print(f"series: {series}, {list(data.keys())}")
            # data.loc['parameter'=='SALT_CTD']
            """
            ['MYEAR', 'PROJ', 'ORDERER', 'SDATE', 'STIME', 'EDATE', 'ETIME', 'CTRYID', 'SHIPC', 'CRUISE_NO', 'SERNO', 'STATN', 'STNCODE', 'LATIT_NOM', 'LONGI_NOM', 'LATIT', 'LONGI', 'POSYS', 'WADEP', 'ADD_SMP', 'COMNT_VISIT', 'COMNT_INTERN', 'WINDIR', 'WINSP', 'AIRTEMP', 'AIRPRES', 'WEATH', 'CLOUD', 'WAVES', 'ICEOB', 'SMPNO', 'DEPH', 'TEMPID_BTL', 'DOXY_CTD_KUST', 'COMNT_SAMP', 'WIRAN', 'MNDEP', 'MXDEP', 'FLOWMETER_READING_START', 'FLOWMETER_READING_STOP', 'FLOWMETER_READING', 'WATER_VOLUME', 'NUM_VIALS', 'SAL_REF', 'TRAWL_TYPE', 'source', 'row_number', 'parameter', 'value', 'quality_flag', 'unit', 'sample_date', 'reported_sample_time', 'date_and_time', 'datetime', 'SERNO_STN']
            """

    def run_automatic_qc(self):
        for parameter in self.parameters:
            print(f"parameter: {parameter.name, parameter.value}")
            for category in self._configuration.categories:
                print(f"category: {category}")
                # Get config for parameter
                if config := self._configuration.get(category, parameter):
                    # Perform all checks
                    QC_CATEGORIES[category](config).check(parameter)

                    # Resync QC-flags with data
                    index, data = parameter.data
                    self._data.iloc[index] = data
