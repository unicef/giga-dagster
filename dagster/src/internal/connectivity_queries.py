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
