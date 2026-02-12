import pandas as pd
from sqlalchemy import text


def get_mng_api_last_update_date():
    from src.utils.db.trino import get_db_context

    with get_db_context() as db:
        result = (
            db.execute(
                text("""SELECT MAX(updated_at) AS last_update_date
                               FROM delta_lake.emis_api_data.mng_api_schools
                            """)
            )
            .mappings()
            .all()
        )

    return result[0]["last_update_date"]


def get_schools_by_govt_id(country_code, id_list):
    from src.utils.db.trino import get_db_context

    with get_db_context() as db:
        result = (
            db.execute(
                text(
                    f"""SELECT * FROM delta_lake.school_master.{country_code}
                         WHERE school_id_govt IN {tuple(id_list)}
                      """  # nosec B608
                )
            )
            .mappings()
            .all()
        )

    return pd.DataFrame.from_records(result)
