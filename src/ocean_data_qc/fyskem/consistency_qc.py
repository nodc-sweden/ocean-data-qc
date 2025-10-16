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
        parameter_boolean = pl.col("parameter") == parameter
        parameters_list_expr = pl.col("parameter").is_in(configuration.parameter_list)

        # Early exit if nothing matches
        if self._data.filter(parameter_boolean).is_empty():
            return

        # Value column as reusable expression
        val = pl.col("value").fill_nan(None)

        # Summation per group (other parameters only)
        summation_expr = (
            pl.when(val.is_not_null().any())  # At least one non-null in group
            .then(val.fill_null(0).sum())  # Sum with null treated as 0
            .otherwise(None)  # Keep None if all null
            .alias("summation")
        )

        summation = (
            self._data.filter(parameters_list_expr)
            .group_by(["visit_key", "DEPH"])
            .agg(summation_expr)
        )

        # Apply TOC conversion logic inline
        toc_unit_conversion = 83.25701  # mg/l → umol/l
        difference_expr = (
            pl.when(pl.col("parameter") == "TOC")
            .then((pl.col("value") * toc_unit_conversion) - pl.col("summation"))
            .otherwise(pl.col("value") - pl.col("summation"))
            .alias("difference")
        )

        # Filter to selection, join summation, compute difference
        selection = (
            self._data.filter(parameter_boolean)
            .join(summation, on=["visit_key", "DEPH"], how="left")
            .with_columns(difference_expr)
        )

        self._apply_flagging_logic(selection, configuration)

    def _apply_flagging_logic(
        self, selection: pl.DataFrame, configuration: ConsistencyCheck
    ) -> pl.DataFrame:
        """
        Apply flagging logic for value vs. summation difference test using polars.
        """
        param_list_str = ", ".join(configuration.parameter_list)

        result_expr = (
            pl.when(pl.col("value").is_null() | pl.col("value").is_nan())
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
            .when(pl.col("summation").is_null() | pl.col("summation").is_nan())
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
                            pl.lit(self._parameter),
                            pl.lit(param_list_str),
                            pl.col("value"),
                            pl.col("summation").round(2),
                            pl.col("difference").round(2),
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
                            "BAD_DATA_CORRECTABLE: difference {}-{} {} - {} = {} "
                            "outside allowed range but within range {}-{}",
                            pl.lit(self._parameter),
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
                            "BAD: difference {}-{} {} - {} = {} "
                            "outside allowed range {}-{}",
                            pl.lit(self._parameter),
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
        )

        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)
