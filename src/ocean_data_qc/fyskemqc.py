import pandas as pd
import polars as pl

from ocean_data_qc.fyskem.consistency_qc import ConsistencyQc
from ocean_data_qc.fyskem.detectionlimit_qc import DetectionLimitQc
from ocean_data_qc.fyskem.h2s_qc import H2sQc
from ocean_data_qc.fyskem.increasedecrease_qc import IncreaseDecreaseQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_configuration import QcConfiguration
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from ocean_data_qc.fyskem.qc_flags import QcFlags
from ocean_data_qc.fyskem.range_qc import RangeQc
from ocean_data_qc.fyskem.spike_qc import SpikeQc
from ocean_data_qc.fyskem.statistic_qc import StatisticQc

QC_CATEGORIES = (
    RangeQc,
    DetectionLimitQc,
    SpikeQc,
    StatisticQc,
    ConsistencyQc,
    H2sQc,
    IncreaseDecreaseQc,
)


class FysKemQc:
    def __init__(self, data: pd.DataFrame):
        self._data = data
        self._configuration = QcConfiguration()
        self._original_flags = self._data["quality_flag_long"].copy()
        self._data[["INCOMING_QC", "AUTO_QC", "MANUAL_QC", "TOTAL_QC"]] = self._data[
            "quality_flag_long"
        ].str.split("_", expand=True)

    def __len__(self):
        return len(self._data)

    def __getitem__(self, index):
        series = self._data.iloc[index]
        return Parameter(series)

    @property
    def parameters(self):
        return {Parameter(series) for _, series in self._data.iterrows()}

    def run_automatic_qc(self):
        ordered_qc_tests = sorted(
            (QcField[category.__name__.removesuffix("Qc")], category)
            for category in QC_CATEGORIES
        )
        for field, qc_category in ordered_qc_tests:
            print(f"run {field.name} qc")
            # Get config for parameter
            category_checker = qc_category(self._data)
            category_checker.expand_qc_columns()

            for parameter in self._configuration.parameters(
                f"{field.name.lower()}_check"
            ):
                if config := self._configuration.get(
                    f"{field.name.lower()}_check", parameter
                ):
                    category_checker.check(parameter, config)

            category_checker.collapse_qc_columns()

        self._update_total()

    def _update_total(self):
        """
        Updates the totalflag in the quality_flag_long string. By supplying the
        quality_flag_long string where the total value in the string is then
        updated through QcFlags
        """
        changed_mask = self._data["quality_flag_long"] != self._original_flags

        if changed_mask.any():
            self._data.loc[changed_mask, "quality_flag_long"] = self._data.loc[
                changed_mask, "quality_flag_long"
            ].apply(lambda x: str(QcFlags.from_string(x)))

    def total_flag_info(self):
        pl_data = pl.from_pandas(self._data)

        # Compute new columns in Polars
        result = pl_data.select(
            [
                pl.col("quality_flag_long")
                .map_elements(lambda x: str(QcFlags.from_string(x).total_automatic))
                .alias("total_automatic"),
                pl.col("quality_flag_long")
                .map_elements(
                    lambda x: "; ".join(
                        [
                            QcFlags.from_string(x).get_field_name(f)
                            for f in QcFlags.from_string(x).total_automatic_source
                        ]
                    )
                )
                .alias("total_automatic_fields"),
                pl.struct(pl_data.columns)
                .map_elements(FysKemQc.extract_info)
                .alias("total_automatic_info"),
            ]
        )

        # Convert result to pandas and add to existing DataFrame
        result_df = result.to_pandas()
        for col in result_df.columns:
            self._data[col] = result_df[col]

    @staticmethod
    def extract_info(row: dict) -> str:
        qcflags = QcFlags.from_string(row["quality_flag_long"])
        fields = qcflags.total_automatic_source
        info_items = []
        for field in fields:
            col_name = f"info_AUTO_QC_{field.name}"
            value = row.get(col_name, None)
            if value is not None:
                info_items.append(f"{field.name}: {value}")
        return "; ".join(info_items)


if __name__ == "__main__":
    # Create the data as a list of dictionaries
    data = [
        {
            "LATIT": 5711.562,
            "LONGI": 1139.446,
            "STATN": "FLADEN",
            "visit_key": "77-10-2024-0005",
            "CTRYID": 77,
            "SHIPC": 10,
            "CRUISE_NO": "02",
            "SERNO": "0005",
            "visit_month": 1,
            "sample_date": "2024-01-11",
            "reported_sample_time": "07:20",
            "sea_basin": "Kattegat",
            "WADEP": 85,
            "DEPH": 20,
            "parameter": "DOXY_CTD",
            "value": 62,
            "quality_flag_long": "1_1101210_0_2",
        },
        {
            "LATIT": 5711.562,
            "LONGI": 1139.446,
            "STATN": "FLADEN",
            "visit_key": "77-10-2024-0005",
            "CTRYID": 77,
            "SHIPC": 10,
            "CRUISE_NO": "02",
            "SERNO": "0005",
            "visit_month": 1,
            "sample_date": "2024-01-11",
            "reported_sample_time": "07:20",
            "sea_basin": "Kattegat",
            "WADEP": 85,
            "DEPH": 20,
            "parameter": "NTRI",
            "value": 0.2,
            "quality_flag_long": "1_0000000_0_2",
        },
        {
            "LATIT": 5711.562,
            "LONGI": 1139.446,
            "STATN": "FLADEN",
            "visit_key": "77-10-2024-0005",
            "CTRYID": 77,
            "SHIPC": 10,
            "CRUISE_NO": "02",
            "SERNO": "0005",
            "visit_month": 1,
            "sample_date": "2024-01-11",
            "reported_sample_time": "07:20",
            "sea_basin": "Kattegat",
            "WADEP": 85,
            "DEPH": 20,
            "parameter": "H2S",
            "value": 80,
            "quality_flag_long": "1_0000000_0_2",
        },
    ]

    # Create the DataFrame
    data = pd.DataFrame(data)
    fyskem_qc = FysKemQc(data)
    fyskem_qc.run_automatic_qc()
    fyskem_qc.total_flag_info()
    print(fyskem_qc._data)
    # qcflags = QcFlags().from_string("0_0400400_0_0")
    # print(qcflags)
    # print(qcflags.automatic)
    # total_flag_index = min(
    #         enumerate([flag for flag in (qcflags.automatic)]),
    # key=lambda x: QcFlag.key_function(x[1]),
    # default=(None, QcFlag.NO_QC_PERFORMED))[0]
    # print(QcField(total_flag_index).name)
    # print(qcflags.total_automatic)
    # print([qcflags.get_field_name(value) for value in qcflags.total_automatic_fields])
