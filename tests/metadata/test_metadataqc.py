from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.visit import Visit
from ocean_data_qc.metadataqc import MetadataQc

from tests.setup_methods import generate_data_frame_of_length


def test_run_checks_for_parameters():
    # Given data for a visit
    given_data = generate_data_frame_of_length(100, number_of_visits=1)
    visit = Visit(given_data)

    # Given a MetadataQc object
    given_metadataqc = MetadataQc(visit)

    # And no metadata CQ has been performed
    assert len(visit.qc)
    assert all(
        metadata_flag == MetadataFlag.NO_QC_PERFORMED
        for metadata_flag in visit.qc.values()
    )

    # When running automatic QC
    given_metadataqc.run_qc()

    # Then all flags are changed
    assert len(visit.qc)
    assert all(
        metadata_flag != MetadataFlag.NO_QC_PERFORMED
        for metadata_flag in visit.qc.values()
    )
