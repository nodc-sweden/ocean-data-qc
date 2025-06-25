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
        GOOD_DATA: difference within lower bounds
            good_lower <= difference <= good_upper_
        BAD_DATA_CORRECTABLE: difference within max bounds
            max_lower <= difference <= max_upper
        BAD_DATA: everything outside the max bounds
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
        Apply flagging logic for value vs. summation difference test using polars.
        """
        pl_selection = pl.from_pandas(selection)
        param_list_str = ", ".join(configuration.parameter_list)

        # Calculate the difference once to avoid repeating the subtraction
        pl_selection = pl_selection.with_columns(
            [(pl.col("value") - pl.col("summation")).alias("difference")]
        )

        result_expr = (
            pl.when(pl.col("value").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.MISSING_VALUE.value)).alias("flag"),
                        pl.format(
                            "MISSING: no value for {}", pl.lit(self._parameter)
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
                            "NO_QC_PERFORMED: {} not available",
                            pl.lit(param_list_str),
                        ).alias("info"),
                    ]
                )
            )
            # GOOD: difference in tight range
            .when(
                (pl.col("difference") >= configuration.good_lower)
                & (pl.col("difference") <= configuration.good_upper)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD: difference {}-{} {} - {} = {} is within {}-{}",
                            pl.lit("parameter"),
                            pl.lit(param_list_str),
                            pl.col("value"),
                            pl.col("summation").round(2),
                            pl.col("difference"),
                            pl.lit(configuration.good_lower),
                            pl.lit(configuration.good_upper),
                        ).alias("info"),
                    ]
                )
            )
            # PROBABLY BAD: in wider (but acceptable) range
            .when(
                (pl.col("difference") >= configuration.max_lower)
                & (pl.col("difference") <= configuration.max_upper)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA_CORRECTABLE.value)).alias("flag"),
                        pl.format(
                            "BAD_DATA_CORRECTABLE: difference {}-{} {} - {} = {} \
                                outside allowed range but within range {}-{}",
                            pl.lit("parameter"),
                            pl.lit(param_list_str),
                            pl.col("value"),
                            pl.col("summation").round(2),
                            pl.col("difference").round(2),
                            pl.lit(configuration.max_lower),
                            pl.lit(configuration.max_upper),
                        ).alias("info"),
                    ]
                )
            )
            # BAD: anything else
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.format(
                            "BAD: difference {}-{} {} - {} = {} \
                                outside allowed range {}-{}",
                            pl.lit("parameter"),
                            pl.lit(param_list_str),
                            pl.col("value"),
                            pl.col("summation").round(2),
                            pl.col("difference"),
                            pl.lit(configuration.max_lower),
                            pl.lit(configuration.max_upper),
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
