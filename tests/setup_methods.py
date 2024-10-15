import uuid
from pathlib import Path

import pandas as pd

from ocean_data_qc.fyskem.qc_checks import DetectionLimitCheck, RangeCheck

PARAMETER_CHOICE = (
    "ALKY",
    "AMON",
    "CNDC_CTD",
    "CPHL",
    "DEPH",
    "DOXY_BTL",
    "DOXY_CTD",
    "H2S",
    "HUMUS",
    "NTOT",
    "NTRA",
    "NTRI",
    "NTRZ",
    "PRES_CTD",
    "SALT_BTL",
    "SALT_CTD",
    "TEMP_BTL",
    "TEMP_CTD",
)


def generate_data_frame(rows: list[dict] = None):
    return pd.DataFrame(rows or [])


def random_number_generator(number_range: tuple = (0, 10), decimal_places: int = 0):
    """Generate a pseudo random integer or float

    The sequence of numbers will always be repeated on all machines. This means that tests
    using these numbers should always behave exactly the same. Algorithm is based on the
    MINSTD method."""

    multiplier = 48271
    increment = 1
    modulo = 2**31 - 1
    previous = (multiplier * increment) % modulo

    while True:
        previous = (multiplier * previous + increment) % modulo
        number = int(
            (previous / (modulo - 1)) * (number_range[1] - number_range[0])
            + number_range[0]
        )
        if decimal_places:
            previous = (multiplier * previous + increment) % modulo
            decimals = int((previous / (modulo - 1)) * (10**decimal_places - 1) + 1)
            number = float(f"{number}.{decimals:02}")
        yield number


def generate_data_frame_of_length(number_of_rows: int, number_of_visits=1):
    rows = []
    random_floats = random_number_generator(number_range=(0, 10), decimal_places=2)
    random_parameter_indices = random_number_generator(
        number_range=(0, len(PARAMETER_CHOICE) - 1)
    )
    random_depth_factors = random_number_generator(number_range=(0, 1), decimal_places=2)

    random_visit = random_number_generator(number_range=(1, number_of_visits))

    for _ in range(number_of_rows):
        value = next(random_floats)
        parameter = PARAMETER_CHOICE[next(random_parameter_indices)]
        visit_id = next(random_visit)
        visit = f"{visit_id:03}"
        wadep = 75 + visit_id * 10
        deph = int(wadep * next(random_depth_factors))
        station = f"Station {visit_id}"
        qc_flag_long = "1_00_0_1"
        rows.append(
            {
                "parameter": parameter,
                "value": value,
                "SERNO": visit,
                "STATN": station,
                "WADEP": wadep,
                "DEPH": deph,
                "quality_flag_long": qc_flag_long,
            }
        )
    return generate_data_frame(rows)


def generate_data_frame_from_data_list(data_list: list[dict], depths: list[int] = None):
    rows = []
    depths = depths or [None]
    random_floats = random_number_generator(number_range=(0, 10), decimal_places=2)
    random_parameter_indices = random_number_generator(
        number_range=(0, len(PARAMETER_CHOICE) - 1)
    )

    for depth in depths:
        for data in data_list:
            value = next(random_floats)
            parameter = PARAMETER_CHOICE[next(random_parameter_indices)]
            row = {"parameter": parameter, "value": value}
            if depth is not None:
                row["DEPH"] = depth
            rows.append(row | data)
    return generate_data_frame(rows)


def generate_data_file_path(dir_path: Path, dataframe: pd.DataFrame = None) -> Path:
    file_path = (dir_path / str(uuid.uuid4())).with_suffix(".csv")
    if not dataframe:
        dataframe = generate_data_frame()
    dataframe.to_csv(file_path)
    return file_path


def generate_range_check_configuration(
    parameter: str, min_range: float, max_range: float
):
    parameter_configuration = RangeCheck(
        min_range_value=min_range, max_range_value=max_range
    )
    return parameter_configuration


def generate_detection_limit_configuration(parameter: str, limit: float):
    parameter_configuration = DetectionLimitCheck(limit=limit)
    return parameter_configuration
