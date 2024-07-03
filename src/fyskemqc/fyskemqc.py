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
    def __init__(self, data: pd.DataFrame):
        """
        data: en dataframe med all data från en station
        """
        self._station_subset = data
        self._configuration = QcConfiguration()

    def __len__(self):
        return len(self._station_subset)

    def __getitem__(self, index):
        series = self._station_subset.loc[index]
        return Parameter(series, index)

    @property
    def values(self):
        return {Parameter(series, index) for index, series in self._station_subset.iterrows()}
    
    @property
    def data(self):
        return self._station_subset
    
    @property
    def updates(self):
        return self._updates
    
    def run_automatic_qc(self):

        """
        
        """
        index_out_of_bounds = False
        for index, series in self._station_subset.iterrows():
            if index > max(self._station_subset.index):
                print(f"Index {index} is out of bounds for DataFrame max index {max(self._station_subset.index)}, min index is {min(self._station_subset.index)}")
                print(f"The DataFrame has len {len(self._station_subset.index)}")
                index_out_of_bounds = True
                break  # eller bryt loopen, beroende på din logik
        print(f"index_out_of_bounds: {index_out_of_bounds}")

        self._updates = {}
        for value in self.values:
            for category in self._configuration.categories:
                # Get config for value
                if config := self._configuration.get(category, value):
                    # Perform all checks
                    QC_CATEGORIES[category](config).check(value)

                    # Resync QC-flags with data, här läggs också quality_flags_long till i data...eller?
                    index, data = value.data                    
                    try:
                        self._updates[index] = value.quality_flag_long
                        # self._station_subset.loc[index, "quality_flag_long"] = value.quality_flag_long
                    except IndexError as e:
                        print(f"Error at index {index}: {e}")
                        print(f"this is the value:\n{data}")
                        print(f"self._data max index {max(self._station_subset.index)}, min index is {min(self._station_subset.index)}")
                        print(f"The DataFrame has len {len(self._station_subset.index)}")
                        print(f"DataFrame segment:\n{self._station_subset.loc[max(0, index-5):index+5]}")
                        raise  # Eller hantera felet på annat sätt