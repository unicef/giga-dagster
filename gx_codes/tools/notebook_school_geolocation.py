# from shapely.geometry import Polygon, Point
# import geopandas

# t1 = Polygon([(4.338074,50.848677), (4.344961,50.833264), (4.366227,50.840809), (4.367945,50.852455), (4.346693,50.858306)])
# t = geopandas.GeoSeries(t1)
# t.crs = 4326

# t2 = geopandas.GeoSeries([Point(4.382617,50.811948)])
# t2.crs = 4326

# dist = t.distance(t2)
# print(dist)


#         # for the schools found outside the boundary:
#         # 1. First, check to see if any is within 1.5km of the country boundary if so then treat it is as within
#         # 2. Use the geopy API to get the country where it is. If the country returned is the correct one, mark is
#         #    as within the country

#         # 1. first check if the points outside the location are within 1.5km from boundary
#         logging.info('Calculating the distance to the country boundary for schools outside')
#         country_polygon = country_gdf['geometry'].explode(index_parts=True)[0][0]
#         point_distances = points_polygon_merge[points_polygon_merge['GID_0'].isna()].apply(
#             lambda r: get_distance_to_boundary(country_polygon, r['geometry']), axis='columns')
#         buffer_country = np.where(point_distances <= 1.5, country, np.nan)
#         logging.info(f'There are {sum(point_distances <= 1.5)} schools within 1.5km and '
#                      f'{len(buffer_country) - sum(point_distances <= 1.5)} are outside the 1.5km buffer')

#         points_polygon_merge.loc[points_polygon_merge['GID_0'].isna(), 'GID_0'] = buffer_country
#         points_polygon_merge['GID_0'].replace('nan', np.nan, inplace=True)

#         logging.info(f"{points_polygon_merge['GID_0'].isna().sum()} schools are still outside country")

#         # get schools we will confirm location with geopy
#         points_polygon_merge['check_geopy'] = points_polygon_merge['GID_0'].isna()

#         # we will only check with geopy if the point is within 150km
#         points_polygon_merge.loc[points_polygon_merge['GID_0'].isna(), 'check_geopy'] = \
#             (points_polygon_merge['check_geopy']) & (point_distances <= 150)

#         if points_polygon_merge['check_geopy'].sum():
#             # 2. Confirm points are actually outside country boundary using geopy (uses Open Street Map)
#             logging.info(f"Checking {points_polygon_merge['check_geopy'].sum()} points with geopy API to confirm if "
#                          f"outside")

#             geolocator = Nominatim(user_agent="schools_geolocation")

#             geopy_countries = points_polygon_merge[points_polygon_merge['check_geopy']]\
#                 .apply(lambda r: get_country_geopy(r['latitude'], r['longitude'], geolocator), axis='columns')

#             geopy_countries.where(geopy_countries == country, np.nan, inplace=True)
#             points_polygon_merge.loc[points_polygon_merge['check_geopy'], 'GID_0'] = geopy_countries
#             points_polygon_merge['GID_0'].replace('nan', np.nan, inplace=True)

#             if points_polygon_merge['GID_0'].isna().sum():
#                 outside_country_df = points_polygon_merge[points_polygon_merge['GID_0'].isna()].drop(
#                     ['internet_speed_Mbps'], axis='columns')
#                 num_schools_outside_boundary = len(outside_country_df)

#     inside_country_df = points_polygon_merge[points_polygon_merge['GID_0'].notna()]
#     num_inside_country_schools = len(inside_country_df)
#     logging.info(f'There are {num_inside_country_schools} schools inside the country')


# import io
# import sys
# import os
# import uuid
# import logging
# import decimal
# import difflib
# import argparse
# from datetime import datetime
# from pathlib import Path

# import country_converter as coco
# import fiona
# import geopandas as gpd
# from geopy.geocoders import Nominatim
# import h3
# from jinja2 import Environment, FileSystemLoader
# import matplotlib.pyplot as plt
# import numpy as np
# import pandas as pd
# from pandas.io.formats import excel
# from sklearn.preprocessing import MinMaxScaler
# from shapely.geometry import Point
# from shapely.ops import nearest_points

# from parameters import CONTAINER_NAME, COVERAGE_CONTAINER_FACEBOOK, COVERAGE_CONTAINER_ITU
# from utils.azure_helpers import download_from_blob_client, upload_to_blob_client


# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.StreamHandler(sys.stdout),
#         logging.FileHandler("geo_location_logs.log")
#     ]
# )

# def get_country_boundary_polygon(country_gadm_file):
#     layer = [layer for layer in fiona.listlayers(country_gadm_file) if layer.endswith("_0")][0]
#     countries_gdf = gpd.read_file(country_gadm_file, layer=layer)
#     country_code = countries_gdf['GID_0'][0]
#     return countries_gdf


# def construct_country_data_folder(country):
#     country_folder = os.path.join('data', '_'.join(country.split()))
#     data_folder = os.path.join(country_folder, datetime.today().strftime('%Y%m%d_%H%M%S'))
#     os.makedirs(data_folder, exist_ok=False)
#     return data_folder


# def clean_df_based_on_location_data(df, country, country_gadm_file):
#     country_gdf = get_country_boundary_polygon(country_gadm_file)

#     # remove the rows with missing lat-long data

#     df['geometry'] = df.apply(lambda row: Point(float(row['longitude']), float(row['latitude'])), axis='columns')
#     points_geo_dataframe = gpd.GeoDataFrame(df)
#     original_num_schools = len(points_geo_dataframe)
#     points_geo_dataframe['valid'] = (~points_geo_dataframe.is_empty) & (points_geo_dataframe.is_valid)

#     missing_location_df = points_geo_dataframe[~points_geo_dataframe['valid']]
#     points_geo_dataframe = points_geo_dataframe[points_geo_dataframe['valid']]
#     num_with_lat_lon = len(points_geo_dataframe)
#     num_schools_without_lat_lon = original_num_schools - num_with_lat_lon
#     logging.info(f'The dataframe was {original_num_schools} and was reduced by {num_schools_without_lat_lon} after '
#                  f'removing missing lat-log to {num_with_lat_lon}')

#     # remove rows with lat-long data outside the country
#     points_polygon_merge = gpd.sjoin(points_geo_dataframe, country_gdf, how='left')

#     num_schools_outside_boundary = 0
#     outside_country_df = pd.DataFrame()
#     if points_polygon_merge['GID_0'].isna().sum():
#         # for the schools found outside the boundary:
#         # 1. First, check to see if any is within 1.5km of the country boundary if so then treat it is as within
#         # 2. Use the geopy API to get the country where it is. If the country returned is the correct one, mark is
#         #    as within the country

#         # 1. first check if the points outside the location are within 1.5km from boundary
#         logging.info('Calculating the distance to the country boundary for schools outside')
#         country_polygon = country_gdf['geometry'].explode(index_parts=True)[0][0]
#         point_distances = points_polygon_merge[points_polygon_merge['GID_0'].isna()].apply(
#             lambda r: get_distance_to_boundary(country_polygon, r['geometry']), axis='columns')
#         buffer_country = np.where(point_distances <= 1.5, country, np.nan)
#         logging.info(f'There are {sum(point_distances <= 1.5)} schools within 1.5km and '
#                      f'{len(buffer_country) - sum(point_distances <= 1.5)} are outside the 1.5km buffer')

#         points_polygon_merge.loc[points_polygon_merge['GID_0'].isna(), 'GID_0'] = buffer_country
#         points_polygon_merge['GID_0'].replace('nan', np.nan, inplace=True)

#         logging.info(f"{points_polygon_merge['GID_0'].isna().sum()} schools are still outside country")

#         # get schools we will confirm location with geopy
#         points_polygon_merge['check_geopy'] = points_polygon_merge['GID_0'].isna()

#         # we will only check with geopy if the point is within 150km
#         points_polygon_merge.loc[points_polygon_merge['GID_0'].isna(), 'check_geopy'] = \
#             (points_polygon_merge['check_geopy']) & (point_distances <= 150)

#         if points_polygon_merge['check_geopy'].sum():
#             # 2. Confirm points are actually outside country boundary using geopy (uses Open Street Map)
#             logging.info(f"Checking {points_polygon_merge['check_geopy'].sum()} points with geopy API to confirm if "
#                          f"outside")

#             geolocator = Nominatim(user_agent="schools_geolocation")

#             geopy_countries = points_polygon_merge[points_polygon_merge['check_geopy']]\
#                 .apply(lambda r: get_country_geopy(r['latitude'], r['longitude'], geolocator), axis='columns')

#             geopy_countries.where(geopy_countries == country, np.nan, inplace=True)
#             points_polygon_merge.loc[points_polygon_merge['check_geopy'], 'GID_0'] = geopy_countries
#             points_polygon_merge['GID_0'].replace('nan', np.nan, inplace=True)

#             if points_polygon_merge['GID_0'].isna().sum():
#                 outside_country_df = points_polygon_merge[points_polygon_merge['GID_0'].isna()].drop(
#                     ['internet_speed_Mbps'], axis='columns')
#                 num_schools_outside_boundary = len(outside_country_df)

#     inside_country_df = points_polygon_merge[points_polygon_merge['GID_0'].notna()]
#     num_inside_country_schools = len(inside_country_df)
#     logging.info(f'There are {num_inside_country_schools} schools inside the country')

#     # drop columns not required and reset index
#     inside_country_df = inside_country_df.drop(columns=['valid', 'index_right', 'GID_0', 'COUNTRY', 'geometry',
#                                                         'check_geopy']).reset_index(drop=True)

#     outside_location_map = None
#     if num_schools_outside_boundary:
#         # finally plot the country polygon and the point=s outside the boundary if any
#         crs = 'epsg:4326'
#         polygon_df = gpd.GeoDataFrame(index=[0], crs=crs, geometry=[country_polygon])
#         ax = polygon_df.plot(figsize=(25, 20), color='xkcd:cyan')
#         outside_country_df.plot(ax=ax, color='xkcd:orange red')

#         plt.axis('off')
#         ax.set_title(f'{country}: Schools outside the country boundary')
#         figure = ax.get_figure()
#         outside_location_map = io.BytesIO()
#         figure.savefig(outside_location_map, format='png')

#     return inside_country_df, num_schools_without_lat_lon, num_schools_outside_boundary, outside_location_map, \
#            missing_location_df, outside_country_df


# def get_country_geopy(lat, lon, geolocator):
#     coords = f"{lat},{lon}"
#     location = None
#     try:
#         location = geolocator.reverse(coords, language='en')
#     except ValueError as e:
#         logging.error(f'The wrong-valued coordinates are: {coords}')
#         if str(e) == 'Must be a coordinate pair or Point':
#             return None
#         else:
#             raise

#     if location:
#         return location.raw['address'].get('country')


# def augment_data(df, country_code, country_name):
#     # df['internet_speed_Mbps'] = df['internet_speed_Mbps'].replace('No', np.nan)
#     if df['internet_speed_Mbps'].dtype == object:
#         df['internet_speed_Mbps'] = df['internet_speed_Mbps'].str.replace('Mbps|GB|MB', '').str.strip().astype(float)

#     df['internet_speed_Mbps'] = df['internet_speed_Mbps'].astype(float).round(2)
#     df['country'] = country_name
#     df['country_code'] = country_code
#     # TODO: Determine if there's a way for this not to be hardcoded
#     df['source'] = "government"

#     df['location_id'] = df['longitude'].astype('str') + "_" + df['latitude'].astype('str')

#     # add the hex id
#     df['hex8'] = df.apply(lambda row: h3.geo_to_h3(float(row['latitude']), float(row['longitude']), resolution=8),
#                           axis='columns')

#     # Broad lat-long
#     df['lat_110'] = df['latitude'].astype(float).map(lambda l: trunc(1000 * float(l)) / 1000)
#     df['long_110'] = df['longitude'].astype(float).map(lambda l: trunc(1000 * float(l)) / 1000)

#     df['len_long'] = df['longitude'].map(lambda lon: -decimal.Decimal(str(lon)).as_tuple().exponent)
#     df['len_lat'] = df['latitude'].map(lambda lat: -decimal.Decimal(str(lat)).as_tuple().exponent)
#     df['len_long'].loc[df['len_long'] > 5] = 5
#     df['len_lat'].loc[df['len_lat'] > 5] = 5

#     df['school_density'] = df.groupby(['hex8'])['giga_id_school'].transform('size')
#     df.drop(columns=['hex8'], inplace=True)

#     return df


# def duplicate_check(df, original_cols):
#     def find_close_duplicates(iterable):
#         already_found = []
#         dup_boolean = []
#         for string_value in iterable:
#             if string_value in already_found:
#                 dup_boolean.append(True)
#                 continue
#             matches = difflib.get_close_matches(string_value, iterable, cutoff=0.7)
#             if len(matches) > 1:
#                 already_found.extend(matches)
#                 dup_boolean.append(True)
#             else:
#                 dup_boolean.append(False)

#         return dup_boolean

#     logging.info('Obtaining basic duplicates')
#     df['duplicate_school_id'] = df.duplicated(subset=['school_id'], keep=False).astype(int)
#     df['dup_id_name_level_location'] = df.duplicated(subset=['school_id', 'school_name', 'education_level',
#                                                              'location_id']).astype(int)
#     df['dup_all_except_school_id'] = df.duplicated(subset=[col for col in original_cols if col != 'school_id']).astype(int)
#     df['dup_school_name_level_location'] = df.duplicated(subset=['school_name', 'education_level', 'location_id']) \
#         .astype(int)
#     df['dup_same_level_location'] = df.duplicated(subset=['education_level', 'location_id']).astype(int)
#     df['dup_same_name_level_within_110m_radius'] = df.duplicated(subset=['school_name', 'education_level',
#                                                                          'lat_110', 'long_110']).astype(int)
#     df['dup_same_name_level_within_110m_radius'].where(
#         ~(
#                 (df['dup_id_name_level_location'] == 1) | (df['dup_all_except_school_id'] == 1) |
#                 (df['dup_school_name_level_location'] == 1) | (df['dup_same_level_location'] == 1)
#         ),
#         0
#     )

#     logging.info('Finding similar duplicates')
#     df['has_similar_name'] = find_close_duplicates(df['school_name'].astype(str).tolist())
#     df['dup_similar_name_same_level_within_100m_radius'] = ((df['has_similar_name']) &
#                                                             (df.duplicated(subset=['school_name', 'lat_110',
#                                                                                    'long_110'])))

#     dup_columns = [column for column in df.columns if column.startswith('dup_')]

#     df['duplicate'] = np.where(df[dup_columns].sum(axis='columns') > 0, 1, 0)

#     df.drop(columns=['location_id', 'lat_110', 'long_110', ], inplace=True)

#     duplicated_df = df[df['duplicate'] == 1]
#     not_duplicated = df[df['duplicate'] == 0]
#     return df, duplicated_df, not_duplicated


# def get_other_stats(df, stats):
#     df['dup_id'] = df.groupby(['school_id'])['school_id'].transform('size')
#     df['dup_id'].loc[(df['dup_id'] == 1) | (df['dup_id'].isna())] = 0
#     df['missing_school_name_flag'] = df['school_name'].isna()
#     stats['num_missing_school_name'] = df['missing_school_name_flag'].sum()
#     stats['num_missing_education_level'] = df['education_level'].isna().sum()

#     try:
#         stats['num_schools_zero_students'] = (df['num_students'] == 0).sum()
#     except KeyError:
#         if 'student_count_boys' in df.columns:
#             df['num_students'] = df['student_count_boys'] + df['student_count_girls']
#             stats['num_schools_zero_students'] = (df['num_students'] == 0).sum()

#     if 'num_students' in df.columns:
#         stats['num_schools_lt_20_students'] = (df['num_students'] == 0).sum()
#         stats['num_schools_gt_2500_students'] = (df['num_students'] == 0).sum()
#         stats['num_schools_student_count_outlier'] = stats['num_schools_lt_20_students'] + \
#             stats['num_schools_gt_2500_students']
#     stats['value_counts_education_level'] = df['education_level'].value_counts().to_string()  # not used
#     df['missing_internet_availability_flag'] = df['internet_availability'].isna()
#     stats['num_missing_internet_availability'] = df['missing_internet_availability_flag'].sum()
#     stats['value_counts_internet_availability'] = df['internet_availability'] \
#         .value_counts().to_string()

#     try:

#         stats['num_missing_mobile_internet_generation'] = df['mobile_internet_generation'].isna().sum()
#         stats['value_counts_mobile_internet_generation'] = df['mobile_internet_generation'].value_counts().to_string()

#     except KeyError:
#         logging.info('No mobile_internet_generation column')

#     if 'internet_type' in df.columns:
#         stats['value_counts_internet_type'] = df['internet_type'].value_counts().to_string()
#         df['missing_internet_type_flag'] = ((df['internet_availability'].str.title().str.strip() == "Yes")
#                                             & (df['internet_type'].isna()))
#         stats['num_missing_type_internet_available'] = df['missing_internet_type_flag'].sum()
#         stats['crosstab_internet_availability_and_type'] = pd.crosstab(df['internet_availability'], df['internet_type'])

#     stats['value_counts_internet_speed'] = df['internet_speed_Mbps'].value_counts().to_string()
#     stats['num_internet_less_than_1mb'] = (
#             (df['internet_availability'] == "Yes") & (df['internet_speed_Mbps'] < 1)).sum()
#     stats['num_internet_more_than_20mb'] = (df['internet_speed_Mbps'] > 200).sum()
#     df['internet_speed_outlier_flag'] = ((df['internet_speed_Mbps'].isna()) & (
#             ((df['internet_availability'] == "Yes") & (df['internet_speed_Mbps'] < 1)) |
#             (df['internet_speed_Mbps'] > 200)
#     ))
#     stats['num_with_internet_speed_outlier'] = df['internet_speed_outlier_flag'].sum()
#     df['missing_internet_speed_Mbps_flag'] = ((df['internet_availability'].str.title().str.strip() == "Yes") &
#                                               (df['internet_speed_Mbps'].isna()))
#     stats['num_missing_speed_internet_available'] = df['missing_internet_speed_Mbps_flag'].sum()

#     try:
#         stats['num_missing_electricity_availability'] = (df['electricity_availability'].isna()).sum()
#     except KeyError:
#         logging.info('No electricity_availability variable')

#     if "computer_availability" not in df.columns and ("computer_count" in df.columns or "num_computers" in df.columns):
#         df['computer_availability'] = np.where(df['num_computers'] > 0, "Yes", "No")

#     if 'computer_availability' in df.columns:
#         stats['num_missing_computer_availability'] = (df['computer_availability'].isna()).sum()
#     else:
#         stats['num_missing_computer_availability'] = df.shape[0]

#     stats['num_missing_school_year'] = (df['school_year'].isna()).sum()

#     # stats['value_counts_school_year'] = df['school_year'].dropna().astype(int).value_counts().to_string()
#     stats['value_counts_school_year'] = df['school_year'].value_counts().to_string()

#     try:
#         stats['num_missing_school_type'] = (df['school_type'].isna()).sum()
#     except KeyError:
#         df['school_type'] = np.nan
#         stats['num_missing_school_type'] = (df['school_type'].isna()).sum()

#     stats['value_counts_school_type'] = df['school_type'].value_counts().to_string()

#     df['school_density_outlier_flag'] = df['school_density'] > 5

#     stats['num_schools_gt_5_700sqm'] = df['school_density_outlier_flag'].sum()

#     # data quality stats (duplicates)
#     stats['total_suspected_duplicates'] = (df['duplicate'] == 1).sum()

#     stats['num_duplicates_except_school_id'] = (df['dup_all_except_school_id'] == 1).sum()

#     stats['num_duplicates_id_name_location_level'] = (df['dup_id_name_level_location'] == 1).sum()

#     stats['num_duplicates_name_level_location'] = (df['dup_school_name_level_location'] == 1).sum()

#     stats['num_duplicates_level_location'] = (df['dup_same_level_location'] == 1).sum()
#     stats['num_same_name_level_within_110m_radius'] = (df['dup_same_name_level_within_110m_radius'] == 1).sum()
#     stats['num_similar_name_same_level_within_100m_radius'] = (df['dup_similar_name_same_level_within_100m_radius']
#                                                                == 1).sum()

#     stats['lat_low_precision'] = (df['len_lat'] < 5).sum()
#     stats['long_low_precision'] = (df['len_long'] < 5).sum()

#     df.drop(columns=['school_density'], inplace=True)

#     return stats, df


# def create_data_quality_report(report_template_location, report_stats):
#     environment = Environment(loader=FileSystemLoader("templates/"))
#     template = environment.get_template("report_template.txt")

#     filled_template = template.render(**report_stats)
#     return filled_template


# def create_error_report(df: pd.DataFrame, dropped_data_df: pd.DataFrame) -> io.BytesIO:

#     # data that will be uploaded but where possible errors have been flagged
#     error_table = df.loc[(df['duplicate'] == 1) | (df['school_id'].duplicated()) |
#                          (df['missing_school_name_flag'] == 1) |
#                          (df['missing_internet_availability_flag'] == 1) | (df['missing_internet_type_flag'] == 1) |
#                          (df['missing_internet_speed_Mbps_flag'] == 1) | (df['internet_speed_outlier_flag'] == 1) |
#                          (df['len_lat'] != 5) | (df['len_long'] != 5)]

#     error_table.drop(columns=['country', 'country_code', 'source', 'giga_id_school'], inplace=True)

#     error_report_file_object = io.BytesIO()

#     excel.ExcelFormatter.header_style = None

#     with pd.ExcelWriter(error_report_file_object) as writer:
#         error_table.to_excel(writer, sheet_name='Uploaded with Quality Flags', index=False)
#         dropped_data_df.to_excel(writer, sheet_name='Not Uploaded', index=False)
#     return error_report_file_object


# def create_master_report(df, original_columns):
#     # Make sure all giga_ids are unique
#     assert df["giga_id_school"].duplicated().sum() == 0, "Giga school ids are not unique"

#     columns_to_use = ['giga_id_school', 'school_id', 'school_name', 'latitude', 'longitude', 'education_level',
#                       'education_level_regional', 'internet_availability', 'internet_type', 'Connectivity type',
#                       'internet_speed_Mbps', 'latency', 'num_students', 'num_teachers', 'num_computers',
#                       'num_classroom', 'address', 'electricity_availability', 'electricity_type', 'water_availability',
#                       'electricity', 'water', 'computer_availability', 'school_year', 'environment', 'admin1', 'admin2',
#                       'admin3', 'admin4', 'school_region', 'type_school','country', 'country_code',
#                       'mobile_internet_generation', 'phone_number', 'email'
#                       ]

#     main_cols = [col for col in columns_to_use if col in df.columns]

#     df = df[main_cols]

#     rename_columns_dict = {'latitude': 'lat',
#                            'longitude': 'lon', 'school_name': 'name',
#                            'internet_availability': 'connectivity',
#                            'internet_type': 'type_connectivity',
#                            'internet_speed_Mbps': 'connectivity_speed',
#                            'latency': 'latency_connectivity',
#                            'electricity_availability': 'electricity',
#                            'water_availability': 'water'
#                            }
#     df.rename(columns=rename_columns_dict, inplace=True)

#     return df


# def read_and_prepare_data(file_path, country_name):
#     if Path(file_path).suffix == '.csv':
#         file_df = pd.read_csv(file_path)
#     elif Path(file_path).suffix == '.xlsx':
#         file_df = pd.read_excel(file_path, engine='openpyxl')
#     else:
#         raise Exception(f'Unsupported file type {Path(file_path).suffix}')

#     # drop rows where all columns are empty
#     file_df = file_df[~file_df.isna().all(axis='columns')]
#     logging.info(f"There are {file_df.shape[0]} schools in the data")

#     if country_name == 'Uzbekistan':
#         file_df['school_name'] = file_df.apply(create_uzbekistan_school_name, axis='columns')

#     if country_name == 'Sierra Leone' and Path(file_path).name == 'School Geolocation Data Merged_04052023.csv':
#         file_df.drop(columns=['school_id'], inplace=True)
#         file_df.rename(columns={'emis_code_school_id': 'school_id'}, inplace=True)


#     file_df.rename(columns={'internet_speed_Mbps_down': 'internet_speed_Mbps',
#                             'student_count': 'num_students',
#                             'name': 'school_name',
#                             'lat': 'latitude',
#                             'lon': 'longitude',
#                             'connectivity_speed': 'internet_speed_Mbps',
#                             'connectivity_availability': 'internet_availability',
#                             'connectivity': 'internet_availability',
#                             'school_ownership': 'school_type',
#                             'Address': 'address'
#                             },
#                    inplace=True)

#     file_df.loc[:, 'school_name'] = file_df['school_name'].str.strip()

#     cols_to_add_if_missing = ['mobile_internet_generation', 'internet_speed_Mbps', 'internet_type',
#                               'internet_availability', 'school_year']

#     for col in cols_to_add_if_missing:
#         if col not in file_df.columns:
#             file_df[col] = np.nan

#     original_cols = file_df.columns.to_list()


#     # Duplicate check
#     num_duplicates = file_df.duplicated(subset='school_id').sum()

#     # Ensure Schools ID is integer (if it is indeed an integer)
#     try:
#         file_df['school_id'] = file_df['school_id'].astype("Int64")
#     except ValueError:
#         pass

#     # Add Giga ID to school
#     file_df['giga_id_school'] = file_df.apply(create_giga_school_id, axis='columns')

#     return file_df, original_cols


# def get_country_gadm_file_name(country_name):
#     country_iso3 = coco.convert(country_name, to='ISO3')
#     gadm_file_name = f"{country_iso3}.gpkg"
#     local_file_path = os.path.join("gadm_files", gadm_file_name)

#     if not os.path.exists(local_file_path):
#         blob_gadm_folder = "source/gadm_files/version4.1"
#         container = "giga"
#         blob_file_path = os.path.join(blob_gadm_folder, gadm_file_name)

#         blob_file_data = download_from_blob_client(container, blob_file_path)

#         with open(local_file_path, 'wb') as f:
#             f.write(blob_file_data)

#     return local_file_path


# def save_file_data(data, file_path, environment='test', container_name=CONTAINER_NAME):

#     if environment == 'test':
#         if type(data) == str:
#             write_params = 'w'
#         elif type(data) == bytes or type(data) == io.BytesIO:
#             write_params = 'wb'
#         else:
#             raise Exception(f'The data type must be either string or bytes')

#         with open(file_path, write_params) as f:
#             if type(data) == io.BytesIO:
#                 data = data.getbuffer()

#             f.write(data)

#     elif environment == 'prod':
#         if type(data) == str:
#             data = data.encode()
#         elif type(data) == bytes:
#             pass
#         elif type(data) == io.BytesIO:
#             data = data.getbuffer()

#         upload_to_blob_client(container=container_name, blob_file_path=file_path, binary_data=data, overwrite=True)


# def run(file_path, country_name, environment):
#     country_code = coco.convert(country_name, to="ISO3")

#     # create the necessary file names
#     report_template_file = "report_template_smaller.txt"
#     report_file_name = f"{country_code}_data_quality_report.txt"
#     error_report_file_name = f"{country_code}_data_quality_error_report.xlsx"
#     master_file_name = f"{country_code}_school_geolocation_master.csv"

#     logging.info('Reading and preparing the data')
#     df, original_cols = read_and_prepare_data(file_path, country_name)
#     num_schools_raw_file = df.shape[0]

#     country_gadm_file = get_country_gadm_file_name(country_name)

#     logging.info('Cleaning the data using location information')
#     df, num_schools_without_lat_lon, num_schools_outside_boundary, outside_location_map, \
#     missing_location_df, outside_country_df = clean_df_based_on_location_data(df, country_name, country_gadm_file)

#     missing_location_df = missing_location_df[[col for col in missing_location_df.columns if col in original_cols]]
#     missing_location_df['error'] = 'Missing Location'
#     outside_country_df = outside_country_df[[col for col in outside_country_df.columns if col in original_cols]]
#     outside_country_df['error'] = 'Outside Country'

#     logging.info('Augment and transform the data, adding useful columns ')
#     df = augment_data(df, country_code, country_name)

#     logging.info('Check for duplicate schools')
#     df, _, _ = duplicate_check(df, original_cols)

#     logging.info('Generate the necessary stats')
#     stats = {}
#     stats['num_unique_schools'] = df['school_id'].nunique()
#     stats['pct_unique_schools'] = round((100 * stats['num_unique_schools']) / (df['school_id'].count()), 2)
#     stats['num_missing_school_id'] = df['school_id'].isna().sum()

#     num_duplicate_school_id = df[(df['duplicate_school_id']==1) & (df['school_id'].notna())].shape[0]
#     num_missing_school_id = df[df['school_id'].isna()].shape[0]

#     schools_duplicated_school_id = df.loc[(df['duplicate_school_id']==1) & (df['school_id'].notna()),
#                                       [col for col in df.columns if col in original_cols]]
#     schools_duplicated_school_id['error'] = 'Duplicated School ID'

#     schools_missing_school_id = df.loc[df['school_id'].isna(), [col for col in df.columns if col in original_cols]]
#     schools_missing_school_id['error'] = 'Missing School ID'

#     dropped_data_df = pd.concat([missing_location_df, outside_country_df, schools_duplicated_school_id,
#                                  schools_missing_school_id])

#     df = df[df['school_id'].notna()]
#     df = df[df['duplicate_school_id']!=1]

#     stats, df = get_other_stats(df, stats)

#     stats['num_duplicate_school_id'] = num_duplicate_school_id
#     stats['num_schools_raw'] = num_schools_raw_file
#     num_schools_master = df.shape[0]
#     stats['num_schools_master'] = num_schools_master
#     stats['num_schools_excluded'] = num_schools_raw_file - num_schools_master
#     stats['num_schools_without_lat_lon'] = num_schools_without_lat_lon
#     stats['num_schools_outside_boundary'] = num_schools_outside_boundary
#     stats['data_file_name'] = os.path.basename(file_path)

#     if environment == 'test':
#         country_data_folder = construct_country_data_folder(country_name)
#         dq_folder = country_data_folder
#         master_folder = country_data_folder
#     elif environment == 'prod':
#         country_data_folder = f"personal/brian/source/School data/Raw data/{country_name}"
#         dq_folder = f"{country_data_folder}/DQ/{datetime.today().strftime('%Y%m%d')}"
#         master_folder = "personal/brian/master/School geo-location master data/"

#     logging.info('Creating the data quality report')
#     report_file_path = os.path.join(dq_folder, report_file_name)
#     dq_report = create_data_quality_report(report_template_file, stats)

#     logging.info('Creating the error report')
#     error_report_path = os.path.join(dq_folder, error_report_file_name)
#     error_report = create_error_report(df, dropped_data_df)

#     logging.info('Creating the master file')
#     master_file_path = os.path.join(master_folder, master_file_name)
#     logging.info(f'Master file is: {master_file_path}')

#     master_df = create_master_report(df, original_cols)

#     save_file_data(master_df.to_csv(index=False), master_file_path, environment=environment)
#     save_file_data(error_report, error_report_path, environment=environment)
#     save_file_data(dq_report.encode(), report_file_path, environment=environment)

#     if outside_location_map:
#         outside_location_map_path = os.path.join(dq_folder, f"{country_code}_outside_location_map.png")
#         save_file_data(outside_location_map, outside_location_map_path, environment=environment)


#     # On Production save files into the partner folders
#     # if environment == 'prod':
#     #     partner_master_file_path = f'unprocessed/{master_file_name}'
#     #     for partner_container in (COVERAGE_CONTAINER_FACEBOOK, COVERAGE_CONTAINER_ITU, COVERAGE_CONTAINER_ERICSSON):
#     #         save_file_data(master_df.to_csv(index=False), partner_master_file_path, environment=environment,
#     #                        container_name=partner_container)


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description='Process a raw school location file and produce error resulting '
#                                                  'reports')
#     parser.add_argument('-p', '--path_to_file', help='The path to the raw file', type=str, required=True)
#     parser.add_argument('-c', '--country_name', help='The country name', type=str, required=True)
#     parser.add_argument('-e', '--environment', help='Whether this is running on Prod or Test', type=str, default='test',
#                         choices=['test', 'prod'])

#     args = parser.parse_args()
#     logging.info(f'Processing data for {args.country_name}')
#     run(args.path_to_file, args.country_name, args.environment)
