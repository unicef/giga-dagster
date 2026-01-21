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
