from pathlib import Path

import pandas as pd
import pytest
from fyskemqc import errors
from fyskemqc.fyskemqc import FysKemQc


def test_instantiate_fyskemqc_with_lims_data():
    # Given LIMS data
    given_ims_data = Path(__file__).parent / "test_data" / "LIMS_data"

    # When creating an FysKemQc object
    fyskemqc = FysKemQc.from_lims_data(given_ims_data)

    # Then there are no error
    assert isinstance(fyskemqc._data, pd.DataFrame)


def test_instantiate_fyskemqc_with_non_existing_lims_data_raises(tmp_path):
    # Given a non-existing directory
    given_non_existing_ims_data = tmp_path / "non_existing_path"
    assert not given_non_existing_ims_data.exists()

    # When creating an FysKemQc object
    # Then an InputDataError is raised
    with pytest.raises(errors.InputDataError):
        FysKemQc.from_lims_data(given_non_existing_ims_data)


def test_instantiate_fyskemqc_with_empty_lims_data_raises(tmp_path):
    # Given an empty directory
    given_empty_ims_data = tmp_path / "empty_path"
    given_empty_ims_data.mkdir()

    # When creating an FysKemQc object
    # Then an InputDataError is raised
    with pytest.raises(errors.InputDataError):
        FysKemQc.from_lims_data(given_empty_ims_data)


def test_instantiate_fyskemqc_with_bad_ims_data_raises(tmp_path):
    # Given an arbitrary file
    given_non_existing_ims_data = tmp_path / "bad_ims_path"
    given_non_existing_ims_data.write_bytes(b"1337")

    # When creating an FysKemQc object
    # Then an InputDataError is raised
    with pytest.raises(errors.InputDataError):
        FysKemQc.from_lims_data(given_non_existing_ims_data)
