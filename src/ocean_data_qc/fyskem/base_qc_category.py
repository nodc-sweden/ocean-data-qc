import abc

import polars as pl

from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flags import QcFlags


class BaseQcCategory(abc.ABC):
    def __init__(self, data: pl.DataFrame, field_position: int, column_name: str):
        self._data = data
        # Ensure we have a stable row identifier
        if "_row_id" not in data.columns:
            self._data = data.with_columns(pl.arange(0, data.height).alias("_row_id"))
        self._field_position = field_position
        self._column_name = column_name
        self._info_column_name = f"info_{column_name}"

    @abc.abstractmethod
    def check(self, parameter: str, configuration): ...

    def expand_qc_columns(self):
        # Add minimal quality flags if missing
        if "quality_flag_long" not in self._data.columns:
            self._data = self._data.with_columns(
                pl.lit(str(QcFlags())).alias("quality_flag_long")
            )

        # Split QC flags to separate columns for incoming, auto and manual
        if not self._data.is_empty():
            flags = pl.col("quality_flag_long").str.split("_")
            self._data = self._data.with_columns(
                [
                    flags.list.get(0).alias("INCOMING_QC"),
                    flags.list.get(1).alias("AUTO_QC"),
                    flags.list.get(2).alias("MANUAL_QC"),
                    flags.list.get(3).alias("TOTAL_QC"),
                ]
            )

        # Add a column for the specific category
        self._data = self._data.with_columns(
            [
                pl.lit(str(QcFlag.NO_QC_PERFORMED.value)).alias(self._column_name),
                pl.lit(str(QcFlag.NO_QC_PERFORMED)).alias(self._info_column_name),
            ]
        )

    def collapse_qc_columns(self):
        # Insert the specific QC flag into the correct position in AUTO_QC
        self._data = self._data.with_columns(
            (
                pl.col("AUTO_QC").str.slice(0, self._field_position)
                + pl.col(self._column_name)
                + pl.col("AUTO_QC").str.slice(self._field_position + 1)
            ).alias("AUTO_QC")
        )

        # Drop the temporary QC column
        self._data = self._data.drop(self._column_name)

        # Recreate quality_flag_long from the four parts
        self._data = self._data.with_columns(
            (
                pl.col("INCOMING_QC")
                + "_"
                + pl.col("AUTO_QC")
                + "_"
                + pl.col("MANUAL_QC")
                + "_"
                + pl.col("TOTAL_QC")
            ).alias("quality_flag_long")
        )

    def update_dataframe(self, selection, result_expr):
        # Add the QC results to the selection
        selection = (
            selection.with_columns([result_expr.alias("result_struct")])
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

        # Collect that data that should be updated in the original dataframe
        # only the two update_cols should be updated
        # (total flag is updated after all checks)
        update_cols = [self._column_name, self._info_column_name]
        update_df = selection.select(["_row_id", *update_cols])

        self._data = self._data.join(
            update_df, on="_row_id", how="left", suffix="_update"
        )

        # Replace columns only where new data exists
        for col in update_cols:
            self._data = self._data.with_columns(
                pl.when(pl.col(f"{col}_update").is_not_null())  # update from qc exists
                .then(pl.col(f"{col}_update"))
                .otherwise(pl.col(col))
                .alias(col)
            ).drop(f"{col}_update")
