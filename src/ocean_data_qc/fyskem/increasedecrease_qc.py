import pandas as pd
import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import IncreaseDecreaseCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class IncreaseDecreaseQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data,
            QcField.IncreaseDecrease,
            f"AUTO_QC_{QcField.IncreaseDecrease.name}",
        )

    def check(self, parameter: str, configuration: IncreaseDecreaseCheck):
        """
        check som kollar förändring från föregående djup av värdet på parameter
        GOOD_DATA: om förändringen ligger mellan allowed_increase och allowed_decrease
        BAD_DATA: om värdet på parameter utanför intervallet
        """
        self._parameter = parameter
        parameter_boolean = self._data.parameter == parameter
        selection = self._data.loc[parameter_boolean]

        # First value (normaly surface) will always be nan.
        selection["difference"] = selection.groupby("visit_key")["value"].diff()
        # Compute the difference grouped by visit_key (like pandas .groupby().diff())
        # selection = selection.sort(["visit_key", "DEPH"]).with_columns([
        #     pl.col("value").diff().over("visit_key").alias("difference")
        # ])
        selection = self._apply_polars_flagging_logic(selection, configuration)
        self._data.loc[parameter_boolean, [self._column_name, self._info_column_name]] = (
            selection[[self._column_name, self._info_column_name]].values
        )

    def _apply_polars_flagging_logic(
        self, selection: pd.DataFrame, configuration: IncreaseDecreaseCheck
    ) -> pd.DataFrame:
        """
        Apply flagging logic for value vs. summation deviation test using polars.
        """
        pl_selection = pl.from_pandas(selection)

        # Create the flag + info struct logic
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
            .when(
                (pl.col("difference") > -configuration.allowed_decrease)
                & (pl.col("difference") < configuration.allowed_increase)
                | pl.col("difference").is_null()
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD change from previous depth {} is within {}-{}",
                            pl.col("difference").round(2),
                            pl.lit(-configuration.allowed_decrease),
                            pl.lit(configuration.allowed_increase),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.format(
                            "BAD change from previous depth {} not within {}-{}",
                            pl.col("difference").round(2),
                            pl.lit(-configuration.allowed_decrease),
                            pl.lit(configuration.allowed_increase),
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

        # self._data.loc[boolean_selection, self._column_name] = np.where(
        #     pd.isna(selection["value"]),
        #     str(QcFlag.MISSING_VALUE.value),
        #     np.where(
        #         np.logical_and(
        #             selection.difference > -configuration.allowed_decrease,
        #             selection.difference < configuration.allowed_increase,
        #         )
        #         | pd.isna(selection.difference),
        #         str(QcFlag.GOOD_DATA.value),
        #         str(QcFlag.BAD_DATA.value),
        #     ),
        # )
