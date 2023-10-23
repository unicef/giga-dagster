import decimal
import difflib  # Can use fuzzywuzzy or thefuzz instead

import geopy.distance
from constants import DUPLICATE_SCHOOL_DISTANCE


# Availability Tests
def is_available(availability):
    return str(availability).strip().lower() == "yes"


def has_value(value):
    return (value is not None) and (value != "")


def has_both(availability, value):
    return is_available(availability) == has_value(value)


# Decimal places tests
def get_decimal_places(number):
    return -decimal.Decimal(str(number)).as_tuple().exponent


def has_at_least_n_decimal_places(number, places):
    return get_decimal_places(number) >= places


# is_within_country(point, country)
# coords_1 = (latitude, longitude)
# coords_2 = (latitude, longitude)
# distance_km is a float number that indicates the minimum distane
def are_points_within_km(coords_1, coords_2, distance_km=DUPLICATE_SCHOOL_DISTANCE):
    return geopy.distance.geodesic(coords_1, coords_2).km <= distance_km


# Checks if the name is not similar to any name in the list
def has_no_similar_name(name_list, similarity_percentage=0.7):
    already_found = []
    is_unique = []
    for string_value in name_list:
        if string_value in already_found:
            is_unique.append(False)
            continue
        matches = difflib.get_close_matches(
            string_value, name_list, cutoff=similarity_percentage
        )
        if len(matches) > 1:
            already_found.extend(matches)
            is_unique.append(False)
        else:
            is_unique.append(True)

    return is_unique


if __name__ == "__main__":
    print(are_points_within_km((12.000, 13.000), (12.000, 13.100), 100))
    print(are_points_within_km((12.000, 13.000), (12.000, 15.100), 100))
    print(has_no_similar_name(["mawda", "adsads"]))
    print(has_no_similar_name(["mawda", "mawda / B"]))
    print(has_at_least_n_decimal_places(10.35452, 5))
    print(has_at_least_n_decimal_places(10.3545, 5))
