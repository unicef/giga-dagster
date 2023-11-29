from src._utils.adls import ADLSFileClient

gold_folder = "raw/school_geolocation_coverage_data/gold/school_data"
silver_folder = "raw/school_geolocation_coverage_data/silver"
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
