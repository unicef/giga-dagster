import pandas as pd
from sqlalchemy import text


def get_rt_schools(iso2_country_code: str, is_test=False) -> pd.DataFrame:
    from src.utils.db.gigamaps import get_db_context

    with get_db_context() as db:
        rt_schools = (
            db.execute(
                text("""
                SELECT
                    DISTINCT sch.giga_id_school school_id_giga,
                    sch.external_id school_id_govt,
                    (min(stat.created) over (partition by stat.school_id)) connectivity_RT_ingestion_timestamp,
                    c.code country_code,
                    c.name country
                FROM public.connection_statistics_schooldailystatus stat
                LEFT JOIN public.schools_school sch ON sch.id = stat.school_id
                LEFT JOIN public.locations_country c ON c.id = sch.country_id
                WHERE c.code = :country_code AND stat.deleted IS NULL AND sch.deleted IS NULL
                LIMIT :limit
                """),
                {"country_code": iso2_country_code, "limit": 10 if is_test else None},
            )
            .mappings()
            .all()
        )

    return pd.DataFrame.from_records(rt_schools)


def get_giga_meter_schools(is_test=False) -> pd.DataFrame:
    from src.utils.db.gigameter import get_db_context

    with get_db_context() as db:
        giga_meter_schools = (
            db.execute(
                text("""
                SELECT
                    DISTINCT giga_id_school school_id_giga,
                    school_id school_id_govt,
                    'daily_checkapp' source
                FROM public.measurements
                WHERE giga_id_school !='' AND LOWER(source) = 'dailycheckapp'
                LIMIT :limit
                """),
                {"limit": 10 if is_test else None},
            )
            .mappings()
            .all()
        )

    return pd.DataFrame.from_records(giga_meter_schools)


def get_mlab_schools(iso2_country_code: str, is_test=False) -> pd.DataFrame:
    from src.utils.db.gigameter import get_db_context

    with get_db_context() as db:
        res = (
            db.execute(
                text("""
                SELECT
                    DISTINCT school_id school_id_govt,
                    (min("timestamp") over (partition by school_id))::DATE mlab_created_date,
                    client_info::JSON ->> 'Country' country_code,
                    'mlab' source
                FROM public.measurements
                WHERE client_info::JSON ->> 'Country' = :country_code AND LOWER(source) = 'mlab'
                LIMIT :limit
                """),
                {"country_code": iso2_country_code, "limit": 10 if is_test else None},
            )
            .mappings()
            .all()
        )

    return pd.DataFrame.from_records(res)


def get_all_gigameter_schools(is_test=False) -> pd.DataFrame:
    from src.utils.db.trino import get_db_context

    with get_db_context() as db:
        giga_meter_schools = (
            db.execute(
                text("""
            SELECT DISTINCT measure.giga_id_school school_id_giga,
                   measure.school_id school_id_govt,
                   MIN(measure.timestamp) OVER (PARTITION BY measure.giga_id_school) first_measurement_timestamp,
                   country.iso3_format country_code
            FROM gigameter_production_db.public.measurements measure
            LEFT JOIN gigamaps_production_db.public.schools_school school ON school.giga_id_school = measure.giga_id_school
            LEFT JOIN gigamaps_production_db.public.locations_country country ON country.id = school.country_id
            WHERE school.deleted IS NULL
              AND LOWER(measure.source) = 'dailycheckapp'
                LIMIT :limit
                """),
                {"limit": 10 if is_test else None},
            )
            .mappings()
            .all()
        )

    return pd.DataFrame.from_records(giga_meter_schools)


def get_all_mlab_schools(is_test=False) -> pd.DataFrame:
    from src.utils.db.trino import get_db_context

    with get_db_context() as db:
        mlab_schools = (
            db.execute(
                text("""
            SELECT DISTINCT school.giga_id_school school_id_giga,
                   measure.school_id school_id_govt,
                   MIN(measure.timestamp) OVER (PARTITION BY school.giga_id_school) first_measurement_timestamp,
                   'mlab' source,
                   country.iso3_format country_code
            FROM gigameter_production_db.public.measurements measure
            JOIN gigamaps_production_db.public.schools_school school ON school.external_id = measure.school_id
            JOIN gigamaps_production_db.public.locations_country country ON country.id = school.country_id 
                 AND country.code = json_query(json_format(measure.client_info), 'strict $.Country' omit quotes)
            WHERE school.deleted IS NULL
              AND LOWER(measure.source) = 'mlab'
                LIMIT :limit
                """),
                {"limit": 10 if is_test else None},
            )
            .mappings()
            .all()
        )

    return pd.DataFrame.from_records(mlab_schools)


def get_qos_schools_by_country(country_iso3_code, is_test=False):
    from src.utils.db.trino import get_db_context

    table_name = f"qos.{country_iso3_code}"

    with get_db_context() as db:
        qos_schools = (
            db.execute(
                text("""
                SELECT DISTINCT school_id_giga,
                       school_id_govt,
                       MIN(timestamp) OVER (PARTITION BY school_id_giga) first_measurement_timestamp
                FROM :table_name
                LIMIT :limit
                """),
                {"table_name": table_name, "limit": 10 if is_test else None},
            )
            .mappings()
            .all()
        )

    return pd.DataFrame.from_records(qos_schools)

