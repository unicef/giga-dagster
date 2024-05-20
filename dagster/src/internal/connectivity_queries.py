import pandas as pd
from sqlalchemy import text


def get_rt_schools(iso2_country_code: str) -> pd.DataFrame:
    from src.utils.db.proco import get_db_context

    with get_db_context() as db:
        rt_schools = (
            db.execute(
                text("""
                SELECT
                    DISTINCT sch.giga_id_school school_id_giga,
                    sch.external_id school_id_govt,
                    (min(stat.created) over (partition by stat.school_id)) connectivity_RT_ingestion_timestamp,
                    c.code AS country_code,
                    c.name country
                FROM connection_statistics_schooldailystatus stat
                LEFT JOIN schools_school sch ON sch.id = stat.school_id
                LEFT JOIN locations_country c ON c.id = sch.country_id
                WHERE country_code = :country_code
                LIMIT 10
                """).bindparams(country_code=iso2_country_code)
            )
            .mappings()
            .all()
        )

    return pd.DataFrame.from_records(rt_schools)


def get_giga_meter_schools() -> pd.DataFrame:
    from src.utils.db.proco import get_db_context

    with get_db_context() as db:
        giga_meter_schools = (
            db.execute(
                text("""
                SELECT
                    DISTINCT dca.giga_id_school school_id_giga,
                    school_id school_id_govt,
                    'daily_checkapp' source
                FROM dailycheckapp_measurements dca
                WHERE dca.giga_id_school !=''
                LIMIT 10
                """)
            )
            .mappings()
            .all()
        )

    return pd.DataFrame.from_records(giga_meter_schools)


def get_mlab_schools(iso2_country_code: str) -> pd.DataFrame:
    from src.utils.db.mlab import get_db_context

    with get_db_context() as db:
        res = (
            db.execute(
                text("""
                SELECT
                    DISTINCT mlab.school_id school_id_govt,
                    (min(mlab."timestamp") over (partition by mlab.school_id))::DATE mlab_created_date,
                    client_info::JSON ->> 'Country' AS country_code,
                    'mlab' source
                FROM public.measurements mlab
                WHERE country_code = :country_code
                LIMIT 10
                """).bindparams(country_code=iso2_country_code)
            )
            .mappings()
            .all()
        )

    return pd.DataFrame.from_records(res)
