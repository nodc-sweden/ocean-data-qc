import polars as pl

from ocean_data_qc.fyskem.consistency_qc import ConsistencyQc
from ocean_data_qc.fyskem.detectionlimit_qc import DetectionLimitQc
from ocean_data_qc.fyskem.gradient_qc import GradientQc
from ocean_data_qc.fyskem.h2s_qc import H2sQc
from ocean_data_qc.fyskem.parameter import Parameter
from ocean_data_qc.fyskem.qc_configuration import QcConfiguration
from ocean_data_qc.fyskem.qc_flag_tuple import QcField
from ocean_data_qc.fyskem.qc_flags import QcFlags
from ocean_data_qc.fyskem.range_qc import RangeQc
from ocean_data_qc.fyskem.repeated_value_qc import RepeatedValueQc
from ocean_data_qc.fyskem.spike_qc import SpikeQc
from ocean_data_qc.fyskem.stability_qc import StabilityQc
from ocean_data_qc.fyskem.statistic_qc import StatisticQc

QC_CATEGORIES = (
    DetectionLimitQc,
    RangeQc,
    StatisticQc,
    RepeatedValueQc,
    StabilityQc,
    GradientQc,
    SpikeQc,
    ConsistencyQc,
    H2sQc,
)


class FysKemQc:
    def __init__(self, data: pl.DataFrame):
        self._data = data
        self._configuration = QcConfiguration()
        self._original_flags = self._data["quality_flag_long"].clone()
        flags = pl.col("quality_flag_long").str.split("_")
        self._data = self._data.with_columns(
            [
                flags.list.get(0).alias("INCOMING_QC"),
                flags.list.get(1).alias("AUTO_QC"),
                flags.list.get(2).alias("MANUAL_QC"),
                flags.list.get(3).alias("TOTAL_QC"),
            ]
        )

    def __len__(self):
        return len(self._data)

    def __getitem__(self, index):
        row_dict = self._data.row(index, named=True)  # Get named dict of the row
        return Parameter(row_dict)

    @property
    def parameters(self):
        return {
            Parameter(self._data.row(i, named=True)) for i in range(self._data.height)
        }

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
            self._data = category_checker._data

        self._update_total()

    def _update_total(self):
        """
        Updates the total flag in the quality_flag_long string.
        Only applies QcFlags.from_string() to changed rows.
        Skips entirely if no changes are detected.
        """
        changed_mask_expr = pl.col("quality_flag_long") != pl.lit(self._original_flags)

        # Check if there are any changes at all
        if self._data.filter(changed_mask_expr).is_empty():
            return

        # Apply update only where needed
        self._data = self._data.with_columns(
            pl.when(changed_mask_expr)
            .then(
                pl.col("quality_flag_long").map_elements(
                    lambda x: str(QcFlags.from_string(x)), return_dtype=pl.Utf8
                )
            )
            .otherwise(pl.col("quality_flag_long"))
            .alias("quality_flag_long")
        )

    def total_flag_info(self):
        self._data = self._data.with_columns(
            [
                # Compute total_automatic
                pl.col("quality_flag_long")
                .map_elements(
                    lambda x: str(QcFlags.from_string(x).total_automatic),
                    return_dtype=pl.Utf8,
                )
                .alias("total_automatic"),
                # Compute total_automatic_fields
                pl.col("quality_flag_long")
                .map_elements(
                    lambda x: "; ".join(
                        QcFlags.from_string(x).get_field_name(f)
                        for f in QcFlags.from_string(x).total_automatic_source
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias("total_automatic_fields"),
                # Compute total_automatic_info from all columns
                pl.struct(self._data.columns)
                .map_elements(FysKemQc.extract_info, return_dtype=pl.Utf8)
                .alias("total_automatic_info"),
            ]
        )

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
    # Skapa Polars DataFrame
    df = pl.DataFrame(data)
    fyskem_qc = FysKemQc(df)
    fyskem_qc.run_automatic_qc()
