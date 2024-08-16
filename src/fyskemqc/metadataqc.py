from fyskemqc.metadata_qc.metadata_qc_field import MetadataQcField
from fyskemqc.metadata_qc.wadep_qc import WadepQc
from fyskemqc.visit import Visit

METADATA_CATEGORIES = {
    MetadataQcField.Wadep: WadepQc,
}


class MetadataQc:
    def __init__(self, visit: Visit):
        self._visit = visit

    def run_qc(self):
        for category in MetadataQcField:
            METADATA_CATEGORIES[category](self._visit).check()
