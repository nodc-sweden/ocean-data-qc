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
        This check is performed when H2S has values and acceptable quality flags
        (i.e. not Q or 4),
        and similarly when the parameter has values and acceptable quality flags

        BAD_DATA: if parameter has values above detection limit,
        and H2S has values above detection limit
        """

        self._parameter = parameter

        flag_boolean = ~pl.col("quality_flag_long").str.contains(r"(?:6|4|Q)")
        value_boolean = pl.col("value").is_not_null()
        parameter_boolean = pl.col("parameter") == parameter
        selection = self._data.filter(parameter_boolean & value_boolean & flag_boolean)

        # Early exit if nothing matches
        if selection.is_empty():
            return

        selection = selection.join(
            self._data.filter(
                (pl.col("parameter") == "H2S") & value_boolean & flag_boolean
            ).select(
                [
                    pl.col("value").alias("h2s"),
                    "visit_key",
                    "DEPH",
                ]
            ),
            on=["visit_key", "DEPH"],
            how="inner",
        )

        # Exit if nothing matches
        if selection.is_empty():
            return

        result_expr = self._apply_flagging_logic(configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(self, configuration: H2sCheck) -> pl.DataFrame:
        """
        Apply the tests logic to selection
        """
        result_expr = pl.struct(
            [
                pl.lit(str(QcFlag.BAD_VALUE.value)).alias("flag"),
                pl.lit(f"BAD {self._parameter} because h2s present").alias("info"),
            ]
        )

        return result_expr
