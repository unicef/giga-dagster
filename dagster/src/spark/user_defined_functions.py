from decimal import Decimal

from h3 import geo_to_h3
from pyspark.sql.functions import udf


def get_decimal_places_udf_factory(precision: int):
    @udf
    def get_decimal_places(value):
        if value is None:
            out = None
        else:
            decimal_places = -Decimal(str(value)).as_tuple().exponent
            out = int(decimal_places < precision)
        return out

    return get_decimal_places


@udf
def point_110(value):
    if value is None:
        point = None
    else:
        point = int(1000 * float(value)) / 1000

    return point


@udf
def h3_geo_to_h3(latitude: float, longitude: float):
    if latitude is None or longitude is None:
        out = "0"
    else:
        out = geo_to_h3(latitude, longitude, resolution=8)

    return out
