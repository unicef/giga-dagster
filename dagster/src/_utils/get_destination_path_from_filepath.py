# from dagster import asset

filepath = "raw/school_geolocation/BEN-school_geolocation-20210923.csv"


# @asset
def get_destination_path_from_filepath(context, filepath):
    if filepath.split("/", 1)[0] == "raw":
        if filepath.split("/")[1] == "qos":
            destination_prefix = "gold/qos"
        else:
            destination_prefix = "bronze"
            print(destination_prefix)

    elif filepath.split("/", 1)[0] == "bronze":
        destination_prefix = "staging/pending_review"

    elif filepath.split("/", 1)[0] == "staging/pending_review":
        destination_prefix = "staging/approved"

    elif filepath.split("/", 1)[0] == "staging/approved":
        destination_prefix = "silver"

    elif filepath.split("/", 1)[0] == "silver":
        destination_prefix = "gold/master_data"

    else:
        # print("unknown source prefix")
        context.log.info("unknown source prefix")

    destination_filepath = destination_prefix + "/" + filepath.split("/", 1)[1]
    context.log.info(destination_filepath)
    return destination_filepath


# get_destination_path_from_filepath(filepath)
