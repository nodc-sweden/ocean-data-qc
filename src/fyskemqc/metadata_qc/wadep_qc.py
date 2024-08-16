from fyskemqc.metadata_qc.base_metadata_qc_category import BaseMetadataQcCategory
from fyskemqc.metadata_qc.metadata_flag import MetadataFlag
from fyskemqc.metadata_qc.metadata_qc_field import MetadataQcField


class WadepQc(BaseMetadataQcCategory):
    def check(self):
        if max(self._visit.water_depths()) >= self._visit.metadata["WADEP"]:
            self._visit.qc[MetadataQcField.Wadep] = MetadataFlag.BAD_DATA
        else:
            self._visit.qc[MetadataQcField.Wadep] = MetadataFlag.GOOD_DATA
