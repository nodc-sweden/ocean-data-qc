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
        the parameter value and the sum of the values in parameter list.
        If STD_UNCERT exists for all parameters,
        the resultant uncertainty of the difference is calculated and
        used to set the limits. Otherwise, default limits are used.
        GOOD_DATA: difference within lower bounds
            good_lower <= difference <= good_upper_
        PROBABLY_BAD_DATA: difference within max bounds
            max_lower <= difference <= max_upper
        BAD_DATA: everything outside the max bounds
        """
        if "STD_UNCERT" not in self._data.columns:
            self._data = self._data.with_columns(
                pl.lit(None).cast(pl.Float64).alias("STD_UNCERT")
            )

        self._parameter = parameter

        print("self._data.dtypes")
        print(self._data)
        print(self._data.dtypes)

        parameter_boolean = (
            (pl.col("parameter") == parameter)
            & (pl.col("value").is_not_null())
            & (pl.col("INCOMING_QC") != QcFlag.BAD_VALUE.value)
        )

        # Early exit if nothing matches
        if self._data.filter(parameter_boolean).is_empty():
            return

        summation = None
        use_uncertainty = parameter in ["TOC", "DOXY_CTD", "SALT_BTL"]
        for idx, parameter_list in enumerate(configuration.parameter_sets):
            # Summation per group (other parameters only)
            df = (
                self._data.filter(
                    pl.col("parameter").is_in(parameter_list)
                    & pl.col("value").is_not_null()
                    & (pl.col("INCOMING_QC") != QcFlag.BAD_VALUE.value)
                )
                .group_by(["visit_key", "DEPH"])
                .agg(
                    pl.col("value").sum().alias("summation_tmp"),
                    pl.when(pl.col("STD_UNCERT").is_not_null().all() & use_uncertainty)
                    .then((pl.col("STD_UNCERT") ** 2).sum())
                    .otherwise(None)
                    .alias("summation_variance_tmp"),
                    pl.concat_str("parameter", separator=", ").alias(
                        "summation_parameters_tmp"
                    ),
                )
            ).select(
                [
                    "visit_key",
                    "DEPH",
                    "summation_tmp",
                    "summation_variance_tmp",
                    "summation_parameters_tmp",
                ]
            )

            # parameter_sets is treated as a prioritized list:
            # the first matching set per visit_key/DEPH is used
            if summation is None:
                summation = df.rename(
                    {
                        "summation_tmp": "summation",
                        "summation_variance_tmp": "summation_variance",
                        "summation_parameters_tmp": "summation_parameters",
                    }
                )
            else:
                summation = (
                    summation.join(df, on=["visit_key", "DEPH"], how="left")
                    .with_columns(
                        pl.when(pl.col("summation").is_null())
                        .then(pl.col("summation_tmp"))
                        .when(pl.col("summation_tmp") > pl.col("summation"))
                        .then(pl.col("summation_tmp"))
                        .otherwise(pl.col("summation"))
                        .alias("summation"),
                        pl.when(pl.col("summation").is_null())
                        .then(pl.col("summation_variance_tmp"))
                        .when(pl.col("summation_tmp") > pl.col("summation"))
                        .then(pl.col("summation_variance_tmp"))
                        .otherwise(pl.col("summation_variance"))
                        .alias("summation_variance"),
                        pl.when(pl.col("summation").is_null())
                        .then(pl.col("summation_parameters_tmp"))
                        .when(pl.col("summation_tmp") > pl.col("summation"))
                        .then(pl.col("summation_parameters_tmp"))
                        .otherwise(pl.col("summation_parameters"))
                        .alias("summation_parameters"),
                    )
                    .select(
                        [
                            "visit_key",
                            "DEPH",
                            "summation",
                            "summation_variance",
                            "summation_parameters",
                        ]
                    )
                )
        summation = summation.with_columns(
            pl.when(pl.col("summation_parameters").is_not_null())
            .then(pl.col("summation_parameters").map_elements(lambda x: ", ".join(x)))
            .otherwise(None)
            .alias("summation_parameters")
        )

        print("summation")
        print(summation.dtypes)
        print(summation)

        # TOC conversion factor
        toc_unit_conversion = 83.25701  # mg/l → umol/l

        # difference calculation logic
        difference_expr = (
            pl.when(pl.col("parameter") == "TOC")
            .then((pl.col("value") * toc_unit_conversion) - pl.col("summation"))
            .otherwise(pl.col("value") - pl.col("summation"))
            .alias("difference")
        )

        uncertainty_expr = (
            pl.when(pl.col("parameter") == "TOC")
            .then(
                (
                    (pl.col("STD_UNCERT") * toc_unit_conversion) ** 2
                    + pl.col("summation_variance")
                ).sqrt()
            )
            .otherwise(
                ((pl.col("STD_UNCERT") ** 2) + pl.col("summation_variance")).sqrt()
            )
            .alias("uncertainty_difference")
        )

        # select data, join summation, compute difference
        selection = (
            self._data.filter(parameter_boolean)
            .select(
                [
                    "_row_id",
                    "visit_key",
                    "DEPH",
                    "parameter",
                    "value",
                    "STD_UNCERT",
                    "INCOMING_QC",
                ]
            )
            .join(summation, on=["visit_key", "DEPH"], how="left")
            .with_columns(
                difference_expr,
                uncertainty_expr,
            )
        )

        result_expr = self._apply_flagging_logic(configuration=configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(self, configuration: ConsistencyCheck) -> pl.DataFrame:
        """
        Apply flagging logic for value vs. summation difference test using polars.
        """
        result_expr = (
            pl.when(pl.col("summation").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.NO_QUALITY_CONTROL.value)).alias("flag"),
                        pl.format(
                            "NO_QC_PERFORMED: {} not available",
                            pl.col("summation_parameters"),
                        ).alias("info"),
                    ]
                )
            )
            # GOOD: within 2 sigma (from deliverer, 95% of normal distribution)
            .when(
                pl.col("uncertainty_difference").is_not_null()
                & (configuration.upper_limit is None)
                & (pl.col("difference") >= -2 * pl.col("uncertainty_difference"))
                & (pl.col("difference") <= 2 * pl.col("uncertainty_difference"))
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_VALUE.value)).alias("flag"),
                        pl.format(
                            "GOOD: difference {}-{} {} - {} = {} is within {}-{}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -2 * pl.col("uncertainty_difference").round(3),
                            2 * pl.col("uncertainty_difference").round(3),
                        ).alias("info"),
                    ]
                )
            )
            # GOOD: between -2 sigma from deliverer and upper_limit
            .when(
                pl.col("uncertainty_difference").is_not_null()
                & (configuration.upper_limit is not None)
                & (pl.col("difference") >= -2 * pl.col("uncertainty_difference"))
                & (pl.col("difference") <= configuration.upper_limit)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_VALUE.value)).alias("flag"),
                        pl.format(
                            "GOOD: difference {}-{} {} - {} = {} is within {}-{}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -2 * pl.col("uncertainty_difference").round(3),
                            pl.lit(configuration.upper_limit),
                        ).alias("info"),
                    ]
                )
            )
            # GOOD: between -2 sigma and 2 sigma from default configuration
            .when(
                pl.col("uncertainty_difference").is_null()
                & (configuration.upper_limit is None)
                & (pl.col("difference") >= -2 * configuration.sigma)
                & (pl.col("difference") <= 2 * configuration.sigma)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_VALUE.value)).alias("flag"),
                        pl.format(
                            "GOOD: difference {}-{} {} - {} = {} is within {}-{}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -2 * configuration.sigma,
                            2 * configuration.sigma,
                        ).alias("info"),
                    ]
                )
            )
            # GOOD: between -2 sigma and upper limit from default configuration
            .when(
                pl.col("uncertainty_difference").is_null()
                & (configuration.upper_limit is not None)
                & (pl.col("difference") >= -2 * configuration.sigma)
                & (pl.col("difference") <= configuration.upper_limit)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_VALUE.value)).alias("flag"),
                        pl.format(
                            "GOOD: difference {}-{} {} - {} = {} is within {}-{}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -2 * configuration.sigma,
                            configuration.upper_limit,
                        ).alias("info"),
                    ]
                )
            )
            # Probably bad value, between 2 and 3 sigma from deliverer
            .when(
                pl.col("uncertainty_difference").is_not_null()
                & (configuration.upper_limit is None)
                & (
                    (
                        (pl.col("difference") >= -3 * pl.col("uncertainty_difference"))
                        & (pl.col("difference") < -2 * pl.col("uncertainty_difference"))
                    )
                    | (
                        (pl.col("difference") > 2 * pl.col("uncertainty_difference"))
                        & (pl.col("difference") <= 3 * pl.col("uncertainty_difference"))
                    )
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "Probably bad: difference {}-{} {} - {} = {} "
                            "is within {}-{} or {}-{}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -3 * pl.col("uncertainty_difference").round(3),
                            -2 * pl.col("uncertainty_difference").round(3),
                            2 * pl.col("uncertainty_difference").round(3),
                            3 * pl.col("uncertainty_difference").round(3),
                        ).alias("info"),
                    ]
                )
            )
            # Probably bad value, between -2 and -3 sigma from deliverer,
            # upper limit exist
            .when(
                pl.col("uncertainty_difference").is_not_null()
                & (configuration.upper_limit is not None)
                & (
                    (pl.col("difference") >= -3 * pl.col("uncertainty_difference"))
                    & (pl.col("difference") < -2 * pl.col("uncertainty_difference"))
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "Probably bad: difference {}-{} {} - {} = {} is within {}-{}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -3 * pl.col("uncertainty_difference").round(3),
                            -2 * pl.col("uncertainty_difference").round(3),
                        ).alias("info"),
                    ]
                )
            )
            # Probably bad value, between 2 and 3 sigma from default configuration
            .when(
                pl.col("uncertainty_difference").is_null()
                & (configuration.upper_limit is None)
                & (
                    (
                        (pl.col("difference") >= -3 * configuration.sigma)
                        & (pl.col("difference") < -2 * configuration.sigma)
                    )
                    | (
                        (pl.col("difference") > 2 * configuration.sigma)
                        & (pl.col("difference") <= 3 * configuration.sigma)
                    )
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "Probably bad: difference {}-{} {} - {} = {} "
                            "is within {}-{} or {}-{}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -3 * configuration.sigma,
                            -2 * configuration.sigma,
                            2 * configuration.sigma,
                            3 * configuration.sigma,
                        ).alias("info"),
                    ]
                )
            )
            # Probably bad value, between -2 and -3 sigma from default configuration,
            # upper limit exist
            .when(
                pl.col("uncertainty_difference").is_null()
                & (configuration.upper_limit is not None)
                & (
                    (pl.col("difference") >= -3 * configuration.sigma)
                    & (pl.col("difference") < -2 * configuration.sigma)
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "Probably bad: difference {}-{} {} - {} = {} is within {}-{}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -3 * configuration.sigma,
                            -2 * configuration.sigma,
                        ).alias("info"),
                    ]
                )
            )
            # Bad value, below -3 sigma or above 3 sigma from deliverer
            .when(
                pl.col("uncertainty_difference").is_not_null()
                & (configuration.upper_limit is None)
                & (
                    (pl.col("difference") < -3 * pl.col("uncertainty_difference"))
                    | (pl.col("difference") > 3 * pl.col("uncertainty_difference"))
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "BAD: difference {}-{} {} - {} = {} is < {} or > {}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -3 * pl.col("uncertainty_difference").round(3),
                            3 * pl.col("uncertainty_difference").round(3),
                        ).alias("info"),
                    ]
                )
            )
            # Bad value, below -3 sigma from deliverer or above upper limit
            .when(
                pl.col("uncertainty_difference").is_not_null()
                & (configuration.upper_limit is not None)
                & (
                    (pl.col("difference") < -3 * pl.col("uncertainty_difference"))
                    | (pl.col("difference") > configuration.upper_limit)
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "BAD: difference {}-{} {} - {} = {} is < {} or > {}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -3 * pl.col("uncertainty_difference").round(3),
                            configuration.upper_limit,
                        ).alias("info"),
                    ]
                )
            )
            # Bad value, below -3 sigma or above 3 sigma from default configuration
            .when(
                pl.col("uncertainty_difference").is_null()
                & (configuration.upper_limit is None)
                & (
                    (pl.col("difference") < -3 * configuration.sigma)
                    | (pl.col("difference") > 3 * configuration.sigma)
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "BAD: difference {}-{} {} - {} = {} is < {} or > {}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -3 * configuration.sigma,
                            3 * configuration.sigma,
                        ).alias("info"),
                    ]
                )
            )
            # Bad value, below -3 sigma from default configuration or above upper limit
            .when(
                pl.col("uncertainty_difference").is_null()
                & (configuration.upper_limit is not None)
                & (
                    (pl.col("difference") < -3 * configuration.sigma)
                    | (pl.col("difference") > configuration.upper_limit)
                )
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "BAD: difference {}-{} {} - {} = {} is < {} or > {}",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                            -3 * configuration.sigma,
                            configuration.upper_limit,
                        ).alias("info"),
                    ]
                )
            )
            # BAD: anything else
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_VALUE.value)).alias("flag"),
                        pl.format(
                            "BAD: unexpected difference {}-{} {} - {} = {} ",
                            pl.lit(self._parameter),
                            pl.col("summation_parameters"),
                            pl.col("value"),
                            pl.col("summation").round(3),
                            pl.col("difference").round(3),
                        ).alias("info"),
                    ]
                )
            )
        )

        return result_expr
