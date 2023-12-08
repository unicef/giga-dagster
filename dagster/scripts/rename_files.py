from src.utils.adls import ADLSFileClient

gold_folder = "raw/school_geolocation_coverage_data/gold/school_data"
silver_subfolders = [
    "raw/school_geolocation_coverage_data/silver/school_data",
    "raw/school_geolocation_coverage_data/silver/coverage_data",
]
bronze_folder = "raw/school_geolocation_coverage_data/bronze"

adls = ADLSFileClient()

# Rename files in gold. Format: {country-code}_school-master.{file-extension}
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

# Rename files in silver. Format: {country-code}_{dataset-type}_school-master.{file-extension}
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
                elif "_school-geolocation_coverage_" in filepath:
                    new_filepath = filepath.replace(
                        "_school-geolocation_coverage_", "_school-coverage_"
                    )
                else:
                    continue
                adls.rename_file(old_filepath=filepath, new_filepath=new_filepath)

# Rename files in bronze. Format: {country_code}_{dataset_type}_{source}_{last_modified}.{file-extension}
bronze_school_folder = bronze_folder + "/" + "school_data"
bronze_coverage_folder = bronze_folder + "/" + "coverage_data"

bronze_coverage_list = adls.list_paths(bronze_coverage_folder)
for file_data in bronze_coverage_list:
    if file_data["is_directory"]:
        continue
    else:
        filepath = file_data["name"]
        country_code = filepath.split("/")[-2]
        dataset_type = "school-coverage"
        filename = filepath.split("/")[-1]
        file_extension = filepath.split(".")[-1]

        parent_folder = filepath.split("/")[-3]
        if parent_folder == "facebook":
            source = "meta"
        elif parent_folder == "itu":
            source = "itu"
        else:
            continue

        properties = adls.get_file_metadata(filepath=filepath)
        metadata = properties["metadata"]
        if "Date_Modified" in metadata:
            last_modified = metadata["Date_Modified"]
        else:
            last_modified = properties["last_modified"].strftime("%Y%m%d-%H%M%S")

        new_filename = (
            f"{country_code}_{dataset_type}_{source}_{last_modified}.{file_extension}"
        )
        if new_filename == filename:
            continue
        else:
            new_filepath = bronze_coverage_folder + "/" + new_filename
            adls.rename_file(old_filepath=filepath, new_filepath=new_filepath)

# Sample: raw/school_geolocation_coverage_data/bronze/school_data/BEN/20220315/Benin_all.xlsx
bronze_geolocation_list = adls.list_paths(bronze_school_folder)
for file_data in bronze_geolocation_list:
    if file_data["is_directory"]:
        continue
    else:
        filepath = file_data["name"]
        country_code = filepath.split("/")[-3]
        dataset_type = "school-geolocation"
        source = "gov"
        filename = filepath.split("/")[-1]
        file_extension = filepath.split(".")[-1]
        last_modified = filepath.split("/")[-2] + "-000000"

        new_filename = (
            f"{country_code}_{dataset_type}_{source}_{last_modified}.{file_extension}"
        )
        if new_filename == filename:
            continue
        else:
            new_filepath = bronze_school_folder + "/" + new_filename
            adls.rename_file(old_filepath=filepath, new_filepath=new_filepath)
