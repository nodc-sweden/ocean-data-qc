from ocean_data_qc.metadata.base_metadata_qc_category import BaseMetadataQcCategory
from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField


class CommonValuesQc(BaseMetadataQcCategory):
    def check(self):
        conflicting_values = {
            key: ", ".join(sorted(map(str, values)))
            for key, values in self._visit.metadata.items()
            if len(values) > 1
        }
        if conflicting_values:
            self._visit.qc[MetadataQcField.CommonValues] = MetadataFlag.BAD_DATA
            for key, values in conflicting_values.items():
                self._visit.log(
                    MetadataQcField.CommonValues, key, f"Conflicting values: {values}"
                )
        else:
            self._visit.qc[MetadataQcField.CommonValues] = MetadataFlag.GOOD_DATA
