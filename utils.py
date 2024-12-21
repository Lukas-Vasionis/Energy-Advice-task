import math

import numpy as np
import pandas as pd
import requests


def validate_location_code(location_code: str, location_type:str):
    """
    Validuoja lokacijos kodą, priklausomai nuo imammų duomenų (prognozės ar istoriniai)
    :param location_code: identifikacinis vietovės kodas
    :param location_type: 'station' arba place
    :return: location_code
    """
    # Getting valid codes
    if location_type == 'station':
        valid_location_codes = requests.get("https://api.meteo.lt/v1/stations").json()
        valid_location_codes = [x['code'] for x in valid_location_codes]
    elif location_type == 'place':
        valid_location_codes = requests.get("https://api.meteo.lt/v1/places").json()
        valid_location_codes = [x['code'] for x in valid_location_codes]


    mapping_codes_w_contexts={
        "station":"historic",
        "place": "forecast"
    }
    # Asserting if location code is in valid codes
    if location_code is None:
        print(f"The {location_type}_code is not defined.\n" 
        f"If you want to get {mapping_codes_w_contexts[location_type]} data, define one of the following {location_type} codes:"
        f"{valid_location_codes}")

    elif location_code not in valid_location_codes:
        print(
            f"""Invalid {location_type}_code: does not exist.
             Select one of the following {location_type} codes: {valid_location_codes}""")
    else:
        return  location_code


def calculate_sleep_time(requests_to_make, max_requests_per_minute=180):
    """
    Function helps to optimese sleep time between requests when extracting data.
    It calculates minimum sleep time to not violate the API constraint of 180 requests per minute
    :param requests_to_make: amount of requests you plan to make
    :param max_requests_per_minute: default 180
    :return: int, sleep time in seconds
    """
    # Calculate the total time window in seconds
    time_window = 60  # 60 seconds in a minute

    # Calculate the sleep time required between requests
    sleep_time = time_window / max_requests_per_minute

    # Adjust sleep time if the planned requests exceed the limit
    if requests_to_make > max_requests_per_minute:
        total_time_needed = math.ceil(requests_to_make / max_requests_per_minute) * time_window
        sleep_time = total_time_needed / requests_to_make

    return sleep_time*1.001 #Making it a bit longer


def interpolate_temp(my_series):

    """
    4 Užduotis
    Algoritmas; pridėti paplidomas none vertes, kad gautume 5min intervalus ir jas užpildome su interpolate funkcija
    :param my_series: pd.Series su numeriniais duomenimis
    :return: None
    """
    # Number of None values (new values) to insert
    n = 11 # there are twelve 5 minute intervals in one hour. There's already one value so adding 11 instead

    # Creating Series with None values interleaved
    series_interpolated = pd.Series(np.concatenate([[v] + [None] * n for v in my_series]))

    series_interpolated=series_interpolated.fillna(np.nan).interpolate()
    print(series_interpolated.head(50).to_string())