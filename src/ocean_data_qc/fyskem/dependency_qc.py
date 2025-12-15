import polars as pl

from ocean_data_qc.fyskem.base_qc_category import BaseQcCategory
from ocean_data_qc.fyskem.qc_checks import DependencyCheck
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField


class DependencyQc(BaseQcCategory):
    def __init__(self, data):
        super().__init__(data, QcField.Dependency, f"AUTO_QC_{QcField.Dependency.name}")

    def check(self, parameter: str, configuration: DependencyCheck):
        """
        This check controls the flags of the parameters in the
        configuration list and will flag the dependent parameter according
        to these following the priority: 4, 3, 2, 1, 0, 9, 8, 7, 6, 5, 0
        """
        priority_list = ["4", "3", "2", "1", "9", "8", "7", "6", "5", "0"]
        dependency_flags = (
            self._data.filter(
                pl.col("parameter").is_in(configuration.parameter_list)
                & pl.col("value").is_not_null()
            )
            .group_by(["visit_key", "DEPH"])
            .agg(pl.col("quality_flag_long").alias("flags_list"))
            .with_columns(pl.col("flags_list").list.join("").alias("combined_flags"))
            .with_columns(
                pl.coalesce(
                    [
                        pl.when(pl.col("combined_flags").str.contains(p)).then(int(p))
                        for p in priority_list
                    ]
                ).alias("dependency_flag")
            )
            .select(["visit_key", "DEPH", "dependency_flag"])
        )

        selection = self._data.filter(
            (pl.col("parameter") == parameter) & pl.col("value").is_not_null()
        ).join(dependency_flags, on=["visit_key", "DEPH"], how="left")

        if selection.is_empty():
            return

        result_expr = self._apply_flagging_logic(configuration=configuration)
        # Update original dataframe with qc results
        self.update_dataframe(selection=selection, result_expr=result_expr)

    def _apply_flagging_logic(self, configuration: DependencyCheck) -> pl.DataFrame:
        """
        Apply flagging logic for dependency test using polars.
        """
        # Create the flag + info struct logic
        result_expr = (
            pl.when((pl.col("dependency_flag") >= 1) & (pl.col("dependency_flag") <= 4))
            .then(
                pl.struct(
                    [
                        pl.col("dependency_flag").cast(pl.Utf8).alias("flag"),
                        pl.format(
                            "Dependent parameter gets the following flag: {}",
                            pl.col("dependency_flag"),
                        ).alias("info"),
                    ]
                )
            )
            .otherwise(
                pl.struct(
                    [
                        pl.lit(str(QcFlag.NO_QUALITY_CONTROL.value)).alias("flag"),
                        pl.format(
                            "No QC performed since associated parameters "
                            "contain flag: {}",
                            pl.col("dependency_flag"),
                        ).alias("info"),
                    ]
                )
            )
        )
        return result_expr
