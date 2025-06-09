import pandas as pd
import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import ConsistencyCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class ConsistencyQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.Consistency, f"AUTO_QC_{QcField.Consistency.name}")

    def check(self, parameter: str, configuration: ConsistencyCheck):
        """
        This check is applied on the difference between
        the parameter value and the sum of the values in parameter list
        GOOD_DATA: difference >= 0
        PROBABLY_GOOD_DATA: lower_deviation <= difference <= upper_deviation
        BAD_DATA: everything outside the deviation bounds
        """

        self._parameter = parameter
        parameter_boolean = self._data.parameter == parameter
        selection = self._data.loc[self._data.parameter == parameter]

        other_selection = self._data.loc[
            self._data.parameter.isin(configuration.parameter_list)
        ]

        summation = (
            other_selection.groupby(["visit_key", "DEPH"])["value"]
            .sum(min_count=1)
            .reset_index(name="summation")
        )
        selection = pd.merge(selection, summation, on=["visit_key", "DEPH"], how="left")

        selection = self._apply_polars_flagging_logic(selection, configuration)
        self._data.loc[parameter_boolean, [self._column_name, self._info_column_name]] = (
            selection[[self._column_name, self._info_column_name]].values
        )

    def _apply_polars_flagging_logic(
        self, selection: pd.DataFrame, configuration: ConsistencyCheck
    ) -> pd.DataFrame:
        """
        Apply flagging logic for value vs. summation deviation test using polars.
        """
        pl_selection = pl.from_pandas(selection)
        param_list_str = ", ".join(configuration.parameter_list)

        # Calculate the difference once to avoid repeating the subtraction
        pl_selection = pl_selection.with_columns(
            [(pl.col("value") - pl.col("summation")).alias("deviation")]
        )

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
            .when(pl.col("summation").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.NO_QC_PERFORMED.value)).alias("flag"),
                        pl.format(
                            "NO_QC_PERFORMED sum not available for {}",
                            pl.lit(param_list_str),
                        ).alias("info"),
                    ]
                )
            )
            .when(pl.col("deviation") >= 0)
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD {} >= sum of {} → {} >= {}",
                            pl.lit(self._parameter),
                            pl.lit(param_list_str),
                            pl.col("value"),
                            pl.col("summation").round(2),
                        ).alias("info"),
                    ]
                )
            )
            .when(
                (pl.col("deviation") >= configuration.lower_deviation)
                & (pl.col("deviation") <= configuration.upper_deviation)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "PROBABLY_GOOD deviation {} - {} = {} within [{}, {}]",
                            pl.col("value"),
                            pl.col("summation").round(2),
                            pl.col("deviation"),
                            pl.lit(configuration.lower_deviation),
                            pl.lit(configuration.upper_deviation),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.format(
                            "BAD {} < sum of {} → {} < {} (deviation out of range)",
                            pl.lit(self._parameter),
                            pl.lit(param_list_str),
                            pl.col("value"),
                            pl.col("summation").round(2),
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
