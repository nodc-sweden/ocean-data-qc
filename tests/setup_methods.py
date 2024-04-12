import uuid
from pathlib import Path

import pandas as pd


def given_data_frame():
    return pd.DataFrame()


def given_data_file_path(dir_path: Path, dataframe: pd.DataFrame = None) -> Path:
    file_path = (dir_path / str(uuid.uuid4())).with_suffix(".csv")
    if not dataframe:
        dataframe = given_data_frame()
    dataframe.to_csv(file_path)
    return file_path
