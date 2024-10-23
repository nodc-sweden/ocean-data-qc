import pandas as pd

from ocean_data_qc.fyskem.qc_checks import (
    ConsistencyCheck,
    DetectionLimitCheck,
    H2sCheck,
    RangeCheck,
)
from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flags import QcFlags

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
    """
    Generate a dataframe

    If rows is provided they will be added to the dataframe otherwise the dataframe will
    be empty.
    """
    return pd.DataFrame(rows or [])


def random_number_generator(number_range: tuple = (0, 10), decimal_places: int = 0):
    """
    Generate a pseudo random integer or float

    The sequence of numbers will always be repeated on all machines. This means that tests
    using these numbers should always behave exactly the same. Algorithm is based on the
    MINSTD method.
    """

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
    """
    Generate a dataframe with a specific number of rows.

    Data will be pseudo random using the possible parameters listed above. Depth will be
    based on a pseudo random factor lower than 1 of the specific WADEP for the station.

    If number_of_visits is given, the data will be divided evenly among the visits.
    """
    rows = []
    random_floats = random_number_generator(number_range=(0, 10), decimal_places=2)
    random_parameter_indices = random_number_generator(
        number_range=(0, len(PARAMETER_CHOICE) - 1)
    )
    random_depth_factors = random_number_generator(number_range=(0, 1), decimal_places=2)

    random_visit = random_number_generator(number_range=(1, number_of_visits + 1))

    for _ in range(number_of_rows):
        value = next(random_floats)
        parameter = PARAMETER_CHOICE[next(random_parameter_indices)]
        visit_id = next(random_visit)
        visit = f"{visit_id:03}"
        wadep = 75 + visit_id * 10
        deph = int(wadep * next(random_depth_factors))
        station = f"Station {visit_id}"
        qc_flag_long = str(QcFlags(QcFlag.GOOD_DATA))
        visit_key = ("20240111_0720_10_FLADEN",)
        rows.append(
            {
                "parameter": parameter,
                "value": value,
                "SERNO": visit,
                "STATN": station,
                "WADEP": wadep,
                "DEPH": deph,
                "quality_flag_long": qc_flag_long,
                "visit_key": visit_key,
            }
        )
    return generate_data_frame(rows)


def generate_data_frame_from_data_list(data_list: list[dict], depths: list[int] = None):
    """
    Generate a dataframe from a list of dictionaries but also generate a pseudo random
    parameter and value for each entry.

    Values in the dictionary will be used for each row. The values in the dictionary will
    be prioritized over any generated values with the same key.

    If a list of depths is given, the dictionaries will be reused for each depth. The
    parameter and value will be generated again for each new depth.
    """
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


def generate_range_check_configuration(
    parameter: str, min_range: float, max_range: float
):
    """
    Generate a RangCheck configration entry.

    Comparable to reading a parameter from a configuration yaml file.
    """
    parameter_configuration = RangeCheck(
        min_range_value=min_range, max_range_value=max_range
    )
    return parameter_configuration


def generate_detection_limit_configuration(parameter: str, limit: float):
    """
    Generate a DetectionLimit configration entry.

    Comparable to reading a parameter from a configuration yaml file.
    """
    parameter_configuration = DetectionLimitCheck(limit=limit)
    return parameter_configuration


def generate_consistency_check_configuration(
    parameter: str,
    parameter_list: list,
    upper_deviation: float = 0,
    lower_deviation: float = -1,
):
    """
    Generate a ConsistencyCheck configration entry.

    Comparable to reading a parameter from a configuration yaml file.
    """
    parameter_configuration = ConsistencyCheck(
        parameter_list=parameter_list,
        upper_deviation=upper_deviation,
        lower_deviation=lower_deviation,
    )
    return parameter_configuration


def generate_h2s_configuration(parameter: str, skip_flag: str):
    """
    Generate a H2sCheck configration entry.

    Comparable to reading a parameter from a configuration yaml file.
    """
    parameter_configuration = H2sCheck(skip_flag)
    return parameter_configuration
