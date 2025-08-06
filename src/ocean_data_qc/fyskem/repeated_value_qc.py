import pandas as pd
import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import RepeatedValueCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class RepeatedValueQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(
            data,
            QcField.RepeatedValue,
            f"AUTO_QC_{QcField.RepeatedValue.name}",
        )

    def check(self, parameter: str, configuration: RepeatedValueCheck):
        """
        GOOD_DATA: first occurrence of a value
        PROBABLY_GOOD_DATA: repeated occurrence of a value
        """
        self._parameter = parameter
        parameter_boolean = (self._data.parameter == parameter) & (
            ~self._data.value.isnull()
        )
        selection = self._data.loc[parameter_boolean]
        if selection.empty:
            return

        selection["difference"] = selection.groupby("visit_key")["value"].diff()

        selection = self._apply_polars_flagging_logic(selection, configuration)
        self._data.loc[parameter_boolean, [self._column_name, self._info_column_name]] = (
            selection[[self._column_name, self._info_column_name]].values
        )

    def _apply_polars_flagging_logic(
        self, selection: pd.DataFrame, configuration: RepeatedValueCheck
    ) -> pd.DataFrame:
        """
        Apply flagging logic for repeated value test using polars.
        """
        pl_selection = pl.from_pandas(selection)

        # Create the flag + info struct logic
        result_expr = (
            pl.when(
                (pl.col("difference").is_null())
                | (pl.col("difference") != configuration.repeated_value)
            )
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "GOOD value",
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.PROBABLY_GOOD_DATA.value)).alias("flag"),
                        pl.format(
                            "PROBABLY GOOD value. The value is identical to the value "
                            "at the sampled depth above.",
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
