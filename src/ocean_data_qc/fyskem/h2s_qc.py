import pandas as pd
import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import H2sCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class H2sQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.H2s, f"AUTO_QC_{QcField.H2s.name}")

    def check(self, parameter: str, configuration: H2sCheck):
        """
        GOOD_DATA: H2S has flag bad or below detection or value isna
        BAD_DATA: all other H2S flags or value not isna
        BELOW_DETECTIONs: given parameter flag BELOW_DETECTION
        """
        self._parameter = parameter
        parameter_boolean = self._data.parameter == parameter
        selection = self._data[parameter_boolean]
        if selection.empty:
            return
        other_selection = self._data[
            (self._data.parameter == "H2S")
            & ~self._data["quality_flag_long"].str.contains("(?:6|4)")
        ].rename(columns={"value": "h2s"})
        selection = pd.merge(
            selection,
            other_selection[["h2s", "visit_key", "DEPH"]],
            on=["visit_key", "DEPH"],
            how="left",
        )

        selection = self._apply_polars_flagging_logic(selection, configuration)
        self._data.loc[parameter_boolean, [self._column_name, self._info_column_name]] = (
            selection[[self._column_name, self._info_column_name]].values
        )

    def _apply_polars_flagging_logic(
        self, selection: pd.DataFrame, configuration: H2sCheck
    ) -> pd.DataFrame:
        """
        Apply the tests logic to selection using polars return pandas dataframe
        """
        pl_selection = pl.from_pandas(selection)

        result_expr = (
            pl.when(pl.col("value").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.MISSING_VALUE.value)).alias("flag"),
                        pl.lit(f"MISSING no value for {self._parameter}").alias("info"),
                    ]
                )
            )
            .when(pl.col("quality_flag_long").str.contains(configuration.skip_flag))
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BELOW_DETECTION.value)).alias("flag"),
                        pl.lit(
                            f"BELOW_DETECTION {self._parameter} is below detection limit"
                        ).alias("info"),
                    ]
                )
            )
            .when(pl.col("h2s").is_null())
            .then(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.GOOD_DATA.value)).alias("flag"),
                        pl.lit("GOOD no h2s present").alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.BAD_DATA.value)).alias("flag"),
                        pl.lit(f"BAD {self._parameter} because h2s present").alias(
                            "info"
                        ),
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
