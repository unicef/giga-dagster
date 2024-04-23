import country_converter as cc


def identify_country_name(country_code: str) -> str:
    coco = cc.CountryConverter()
    data = coco.data
    return data[data["ISO3"] == country_code]["name_short"].to_list()[0]
