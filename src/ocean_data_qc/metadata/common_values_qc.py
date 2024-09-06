from ocean_data_qc.metadata.base_metadata_qc_category import BaseMetadataQcCategory
from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField


class CommonValuesQc(BaseMetadataQcCategory):
    def check(self):
        multivalued_fields = [
            key for key, value in self._visit.metadata.items() if len(value) > 1
        ]
        if multivalued_fields:
            self._visit.qc[MetadataQcField.CommonValues] = MetadataFlag.BAD_DATA
        else:
            self._visit.qc[MetadataQcField.CommonValues] = MetadataFlag.GOOD_DATA
