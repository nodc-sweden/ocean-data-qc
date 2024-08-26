from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField
from ocean_data_qc.metadata.visit import Visit
from ocean_data_qc.metadata.wadep_qc import WadepQc

METADATA_CATEGORIES = {
    MetadataQcField.Wadep: WadepQc,
}


class MetadataQc:
    def __init__(self, visit: Visit):
        self._visit = visit

    def run_qc(self):
        for category in MetadataQcField:
            METADATA_CATEGORIES[category](self._visit).check()
