import pytest
from ocean_data_qc.metadata.metadata_flag import MetadataFlag
from ocean_data_qc.metadata.metadata_qc_field import MetadataQcField
from ocean_data_qc.metadata.position_qc import PositionQc
from ocean_data_qc.metadata.visit import Visit

from tests.setup_methods import generate_data_frame_from_data_list


@pytest.mark.parametrize(
    "given_latitude, given_longitude, expected_flag",
    (
        ("6220074.671", "594470.389", MetadataFlag.GOOD_DATA),  # SWEREF99 TM
        ("5606.984", "1631.162", MetadataFlag.GOOD_DATA),  # WGS84 DDDMM.SSS
        ("5606.984°", "1631.162°", MetadataFlag.BAD_DATA),  # WGS84 DDDMM.SSS°
        ("56.1164000°", "016.5193667°", MetadataFlag.BAD_DATA),  # WGS84 DDD.DDDDD°
        ("56.1164000", "016.5193667", MetadataFlag.BAD_DATA),  # WGS84 DDD.DDDDD
        ("56°06.98400'", "016°31.16200'", MetadataFlag.BAD_DATA),  # WGS84 DDD° MM.MMM'
        ("56 06.98400", "016 31.16200", MetadataFlag.BAD_DATA),  # WGS84 DDD MM.MMM
        (
            "56°06'59.0400",
            "016°31'09.7200",
            MetadataFlag.BAD_DATA,
        ),  # SWGS84 DDD° MM' SS.S"
        ("56 06 59.0400", "016 31 09.7200", MetadataFlag.BAD_DATA),  # SWGS84 DDD MM SS.S
    ),
)
def test_position_check_for_various_position_formats(
    given_latitude, given_longitude, expected_flag
):
    # Given data with given position values
    given_data = generate_data_frame_from_data_list(
        [{"LATIT": given_latitude, "LONGI": given_longitude}]
    )

    # Given no qc has been made for visit
    visit = Visit(given_data)
    assert visit.qc[MetadataQcField.Position] == MetadataFlag.NO_QC_PERFORMED

    # When performing QC
    date_qc = PositionQc(visit)
    date_qc.check()

    # Then the parameter is given the expected flag at the expected position
    assert visit.qc[MetadataQcField.Position] == expected_flag

    if expected_flag == MetadataFlag.GOOD_DATA:
        # And if good data is expected, the qc log is empty
        assert not visit.qc_log
    else:
        # And if bad data is expected, the value is added to the qc log
        assert (
            "LATIT" in visit.qc_log[MetadataQcField.Position]
            and "LONGI" in visit.qc_log[MetadataQcField.Position]
        )
        assert any(
            given_latitude in entry
            for entry in visit.qc_log[MetadataQcField.Position]["LATIT"]
        )
        assert any(
            given_longitude in entry
            for entry in visit.qc_log[MetadataQcField.Position]["LONGI"]
        )


@pytest.mark.parametrize(
    "given_latitude, given_longitude, expected_latitude, expected_longitude",
    (
        (6220074.671, 594470.38, 5606.984, 1631.162),
        (6419896, 280076, 5752.0003, 1117.5203),
        (6509836, 674098, 5841.6159, 1800.2578),
        (6097873, 391355, 5500.9498, 1318.0500),
    ),
)
def test_sweref99tm_to_wgs84(
    given_latitude, given_longitude, expected_latitude, expected_longitude
):
    # When transforming the SWEREF99TM position
    latitude, longitude = PositionQc.sweref99tm_to_wgs84(given_latitude, given_longitude)

    # Then the result is the expected WGS84 position
    assert latitude == pytest.approx(expected_latitude, abs=1e-4)
    assert longitude == pytest.approx(expected_longitude, abs=1e-4)


#  53.8-66 degN, 4-31 degE
@pytest.mark.parametrize(
    "given_latitude, given_longitude, expected_flag",
    (
        ("5954.0", "1730.0", MetadataFlag.GOOD_DATA),  # Center
        ("6600.0", "400.0", MetadataFlag.GOOD_DATA),  # NW corner
        ("6600.0", "3100.0", MetadataFlag.GOOD_DATA),  # NE corner
        ("5348.0", "400.0", MetadataFlag.GOOD_DATA),  # SW corner
        ("5348.0", "3100.0", MetadataFlag.GOOD_DATA),  # SE corner
        ("6600.001", "1730.0", MetadataFlag.BAD_DATA),
        ("5347.999", "1730.0", MetadataFlag.BAD_DATA),
        ("5954.0", "359.999", MetadataFlag.BAD_DATA),
        ("5954.0", "3100.001", MetadataFlag.BAD_DATA),
    ),
)
def test_position_check_within_rough_area(given_latitude, given_longitude, expected_flag):
    # Given data with given position values
    given_data = generate_data_frame_from_data_list(
        [{"LATIT": given_latitude, "LONGI": given_longitude}]
    )

    # Given no qc has been made for visit
    visit = Visit(given_data)
    assert visit.qc[MetadataQcField.Position] == MetadataFlag.NO_QC_PERFORMED

    # When performing QC
    date_qc = PositionQc(visit)
    date_qc.check()

    # Then the parameter is given the expected flag at the expected position
    assert visit.qc[MetadataQcField.Position] == expected_flag

    if expected_flag == MetadataFlag.GOOD_DATA:
        # And if good data is expected, the qc log is empty
        assert not visit.qc_log
    else:
        # And if bad data is expected, the value is added to the qc log
        assert (
            "LATIT" in visit.qc_log[MetadataQcField.Position]
            and "LONGI" in visit.qc_log[MetadataQcField.Position]
        )
        assert any(
            given_latitude in entry
            for entry in visit.qc_log[MetadataQcField.Position]["LATIT"]
        )
        assert any(
            given_longitude in entry
            for entry in visit.qc_log[MetadataQcField.Position]["LONGI"]
        )
