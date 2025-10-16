import polars as pl
import pytest

from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField, QcFlagTuple
from ocean_data_qc.fyskem.qc_flags import QcFlags
from ocean_data_qc.fyskemqc import QC_CATEGORIES, FysKemQc
from tests.setup_methods import generate_data_frame, generate_data_frame_of_length


@pytest.mark.parametrize("given_number_of_rows", (1, 42, 99))
def test_fyskemqc_length(given_number_of_rows):
    # Given data with given number of rows
    given_data = generate_data_frame_of_length(given_number_of_rows)
    assert len(given_data) == given_number_of_rows

    # When adding the data to FysKemQc object
    fyskemqc = FysKemQc(given_data)

    # Then it exposes the length of the data
    assert len(fyskemqc) == given_number_of_rows


def test_run_checks_for_parameters():
    # Given data
    given_data = generate_data_frame_of_length(5)

    # Given FysKemQc object
    given_fyskemeqc = FysKemQc(given_data)

    # And no automatic CQ has been performed
    assert len(given_fyskemeqc)
    assert all(
        qc_flag == QcFlag.NO_QC_PERFORMED
        for parameter in given_fyskemeqc.parameters
        for qc_flag in parameter.qc.automatic
    )

    # When running automatic QC
    given_fyskemeqc.run_automatic_qc()
    parameter_1 = given_fyskemeqc[0]
    parameter_2 = given_fyskemeqc[1]
    parameter_3 = given_fyskemeqc[2]
    parameter_4 = given_fyskemeqc[3]
    parameter_5 = given_fyskemeqc[4]

    # Then Range QC flags are changed
    assert parameter_1.qc.automatic[QcField.Range] != QcFlag.NO_QC_PERFORMED
    assert parameter_2.qc.automatic[QcField.Range] != QcFlag.NO_QC_PERFORMED
    assert parameter_3.qc.automatic[QcField.Range] != QcFlag.NO_QC_PERFORMED
    assert parameter_4.qc.automatic[QcField.Range] != QcFlag.NO_QC_PERFORMED
    assert parameter_5.qc.automatic[QcField.Range] != QcFlag.NO_QC_PERFORMED


@pytest.mark.parametrize(
    "given_incoming_qc, given_manual_qc",
    (
        (QcFlag.GOOD_DATA, QcFlag.PROBABLY_GOOD_DATA),
        (QcFlag.BAD_DATA, QcFlag.BAD_DATA_CORRECTABLE),
        (QcFlag.VALUE_CHANGED, QcFlag.BELOW_DETECTION),
        (QcFlag.VALUE_IN_EXCESS, QcFlag.INTERPOLATED_VALUE),
        (QcFlag.MISSING_VALUE, QcFlag.MISSING_VALUE),
    ),
)
def test_automatic_qc_does_not_alter_incoming_or_manual_qc(
    given_incoming_qc,
    given_manual_qc,
):
    # Given data with incoming and manual CQ flags
    given_qc = QcFlags(given_incoming_qc, QcFlagTuple(), given_manual_qc)
    given_data = generate_data_frame(
        [
            {
                "parameter": "AMON",
                "value": 0.01,
                "quality_flag_long": str(given_qc),
                "visit_key": "20240111_0720_10_FLADEN",
                "DEPH": 5,
                "sea_basin": "Kattegat",
                "visit_month": "01",
            },
            {
                "parameter": "AMON",
                "value": 200,
                "quality_flag_long": str(given_qc),
                "visit_key": "20240111_0720_10_FLADEN",
                "DEPH": 150,
                "sea_basin": "Kattegat",
                "visit_month": "01",
            },
        ]
    )

    # Given FysKemQc object
    given_fyskemeqc = FysKemQc(given_data)

    # When performing auto CQ
    given_fyskemeqc.run_automatic_qc()

    # Then the original incoming flags are preserved
    parameter_1 = given_fyskemeqc[0]
    parameter_2 = given_fyskemeqc[1]

    assert parameter_1.qc.incoming == given_incoming_qc
    assert parameter_1.qc.manual == given_manual_qc

    assert parameter_2.qc.incoming == given_incoming_qc
    assert parameter_2.qc.manual == given_manual_qc


def test_qc_categories_match_qcfield():
    # Derive expected class names from QcField: add 'Qc' suffix
    expected_classes = {f"{field.name}Qc" for field in QcField}

    # Get actual class names from QC_CATEGORIES
    actual_classes = {cls.__name__ for cls in QC_CATEGORIES}

    missing_in_qc_categories = expected_classes - actual_classes
    unexpected_in_qc_categories = actual_classes - expected_classes

    error_messages = []
    if missing_in_qc_categories:
        error_messages.append(
            f"Missing in QC_CATEGORIES: {sorted(missing_in_qc_categories)}"
        )
    if unexpected_in_qc_categories:
        error_messages.append(
            f"Unexpected in QC_CATEGORIES: {sorted(unexpected_in_qc_categories)}"
        )

    assert not error_messages, (
        "Mismatch between QcField and QC_CATEGORIES:\n" + "\n".join(error_messages)
    )


@pytest.mark.parametrize(
    "given_incoming_qc, expected_total, expected_auto",
    (
        (QcFlag.GOOD_DATA, QcFlag.BELOW_DETECTION, QcFlag.BELOW_DETECTION),
        (QcFlag.BAD_DATA, QcFlag.BAD_DATA, QcFlag.BELOW_DETECTION),
    ),
)
def test_qc_returns_new_flags_in_df(given_incoming_qc, expected_total, expected_auto):
    # Given data with incoming and manual CQ flags
    given_qc = QcFlags(given_incoming_qc, QcFlagTuple())
    given_data = generate_data_frame(
        [
            {
                "parameter": "AMON",
                "value": 0.01,
                "quality_flag_long": str(given_qc),
                "visit_key": "20240111_0720_10_FLADEN",
                "DEPH": 5,
                "sea_basin": "Kattegat",
                "visit_month": "01",
            },
        ]
    )

    # Given FysKemQc object
    given_fyskemeqc = FysKemQc(given_data)

    # When performing auto CQ
    given_fyskemeqc.run_automatic_qc()

    # Then the original incoming flags are preserved
    parameter_1 = given_fyskemeqc[0]

    assert parameter_1.qc.incoming == given_incoming_qc
    assert parameter_1.qc.total_automatic == expected_auto
    assert parameter_1.qc.total == expected_total


# -------------------------
# Fixture: large test dataset
# -------------------------
@pytest.fixture
def large_dataset():
    df = generate_data_frame_of_length(number_of_rows=500, number_of_visits=10)
    # n_rows = 1000  # large enough to mimic your production case

    # # Create example columns; include `parameter` and the QC columns
    # df = pl.DataFrame({
    #     "row_number": list(range(n_rows)),
    #     "parameter": ["TEMP"]*500 + ["SAL"]*500,  # two different parameters
    #     "value": [10.0]*250 + [100.0]*250 + [35.0]*250 + [1000.0]*250,
    #     "AIRPRES": [1013]*n_rows,
    #     "quality_flag": [""]*n_rows,
    #     "info_AUTO_QC_Range": [""]*n_rows,
    #     # ... add any other columns your QC expects ...
    # })

    return df


# -------------------------
# Test: run full QC
# -------------------------
def test_fyskem_qc_large_dataset(large_dataset):
    fyskemeqc = FysKemQc(large_dataset)

    # This should run without shape errors
    try:
        fyskemeqc.run_automatic_qc()
    except Exception as e:
        pytest.fail(f"QC pipeline failed: {e}")

    # Optionally, check that the QC columns were updated
    df_out = fyskemeqc._data  # or however you access the resulting dataframe
    assert "quality_flag_long" in df_out.columns
    assert "info_AUTO_QC_Range" in df_out.columns

    # Ensure that for all TEMP rows, QC flags were applied
    temp_rows = df_out.filter(pl.col("parameter") == "TEMP")
    assert (
        temp_rows.filter(pl.col("quality_flag_long") == "").height == 0
    )  # all TEMP rows flagged
