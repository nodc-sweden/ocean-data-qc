from pathlib import Path

import pytest
from fyskemqc import errors
from fyskemqc.fyskemqc import FysKemQc

from tests import setup_methods


def test_fyskemqc_can_be_instantiated_with_a_file_path_string(tmp_path):
    # Given the path string to a data file
    given_file_path = setup_methods.given_data_file_path(tmp_path)
    given_file_path_string = str(given_file_path)

    # When instantiating FysKemQc
    fyskemqc = FysKemQc.from_csv(given_file_path_string)

    # Then there is no exception
    assert fyskemqc


def test_fyskemqc_instantiated_with_non_existing_file_path_string_raises(tmp_path):
    # Given a file that doesn't exist
    given_non_existing_file_path = tmp_path / "this_file_does_not_exist.txt"

    # When instantiating FysKemQc
    # Then an InputDataError is raised
    with pytest.raises(errors.InputDataError):
        FysKemQc.from_csv(given_non_existing_file_path)


def test_fyskemqc_can_be_instantiated_with_a_path_object(tmp_path):
    # Given the path string to a data file
    given_file_path = setup_methods.given_data_file_path(tmp_path)

    # When instantiating FysKemQc
    fyskemqc = FysKemQc.from_csv(given_file_path)

    # Then there is no exception
    assert fyskemqc


def test_fyskemqc_instantiated_with_non_existing_file_path_raises():
    # Given a file that doesn't exist
    given_non_existing_file_path = Path("this_file_does_not_exist.txt")
    assert not given_non_existing_file_path.exists()

    # When instantiating FysKemQc
    # Then an InputDataError is raised
    with pytest.raises(errors.InputDataError):
        FysKemQc.from_csv(given_non_existing_file_path)
