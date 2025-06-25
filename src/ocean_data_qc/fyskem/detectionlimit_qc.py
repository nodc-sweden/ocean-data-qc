import pandas as pd
import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import DetectionLimitCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class DetectionLimitQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data, QcField.DetectionLimit, f"AUTO_QC_{QcField.DetectionLimit.name}"
        )

    def check(self, parameter, configuration: DetectionLimitCheck):
        """
        BELOW_DETECTION: everything
            - below detection limit
            and
            - at detection limit with incoming flag BELOW_DETECTION
        GOOD_DATA: everything above detection limit
        PROBABLY_GOOD_DATA: everything at detectionlimit without flag
        """
        self._parameter = parameter
        parameter_boolean = self._data.parameter == parameter
        selection = self._data.loc[parameter_boolean]

        selection = self._apply_polars_flagging_logic(selection, configuration)
        self._data.loc[parameter_boolean, [self._column_name, self._info_column_name]] = (
            selection[[self._column_name, self._info_column_name]].values
        )

    def _apply_polars_flagging_logic(
        self, selection: pd.DataFrame, configuration: DetectionLimitCheck
    ) -> pd.DataFrame:
        """
        Apply flagging logic for value vs. summation deviation test using polars.
        """
        pl_selection = pl.from_pandas(selection)

        result_expr = (
            pl.when(pl.col("value").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.MISSING_VALUE.value)).alias("flag"),
                        pl.format(
                            "MISSING no value for {}", pl.lit(self._parameter)
                        ).alias("info"),
                    ]
                )
            )
            .when(pl.col("value") > configuration.limit)
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD value {} > detection limit {}",
                            pl.col("value").round(2),
                            pl.lit(configuration.limit),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (pl.col("value") == configuration.limit)
                & (~pl.col("quality_flag_long").str.contains("6"))
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "PROBABLY_GOOD value == detection limit {} with no flag '6'",
                            pl.lit(configuration.limit),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BELOW_DETECTION.value)).alias("flag"),
                        pl.format(
                            "BELOW_DETECTION "
                            "value ≤ detection limit {} or flagged as '6'",
                            pl.lit(configuration.limit),
                        ).alias("info"),
                    ]
                )
            )
        )

        pl_selection = (
            pl_selection.with_columns([result_expr.alias("result_struct")])
            .with_columns(
                [
                    pl.col("result_struct").struct.field("flag").alias(self._column_name),
                    pl.col("result_struct")
                    .struct.field("info")
                    .alias(self._info_column_name),
                ]
            )
            .drop("result_struct")
        )

        return pl_selection.to_pandas()
