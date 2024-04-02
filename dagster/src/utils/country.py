from country_converter import CountryConverter


def get_country_codes_list():
    coco = CountryConverter()
    return coco.data["ISO3"].to_list()
