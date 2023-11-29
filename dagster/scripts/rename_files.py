from src._utils.adls import ADLSFileClient

gold_folder = "raw/school_geolocation_coverage_data/gold/school_data"
silver_subfolders = [
    "raw/school_geolocation_coverage_data/silver/school_data",
    "raw/school_geolocation_coverage_data/silver/coverage_data",
]
bronze_folder = "raw/school_geolocation_coverage_data/bronze"

adls = ADLSFileClient()

# Rename files in gold
gold_file_list = adls.list_paths(gold_folder)
for file_data in gold_file_list:
    filepath = file_data["name"]
    filename = filepath.split("/")[-1]
    country_code = filename.split(sep="_")[0]
    file_extension = filepath.split(sep=".")[-1]
    new_filename = country_code + "_" + "school-master" + "." + file_extension
    if new_filename == filename:
        continue
    else:
        new_filepath = gold_folder + "/" + new_filename
        adls.rename_file(old_filepath=filepath, new_filepath=new_filepath)

# Rename files in silver
for folder in silver_subfolders:
    silver_file_list = adls.list_paths(folder)
    for file_data in silver_file_list:
        if file_data["is_directory"]:
            continue
        else:
            filepath = file_data["name"]
            if (
                "_school_geolocation_" not in filepath
                and "_school-geolocation_coverage_" not in filepath
            ):
                continue
            else:
                if "_school_geolocation_" in filepath:
                    new_filepath = filepath.replace(
                        "_school_geolocation_", "_school-geolocation_"
                    )
                    print("***")
                elif "_school-geolocation_coverage_" in filepath:
                    new_filepath = filepath.replace(
                        "_school-geolocation_coverage_", "_school-coverage_"
                    )
                adls.rename_file(old_filepath=filepath, new_filepath=new_filepath)
