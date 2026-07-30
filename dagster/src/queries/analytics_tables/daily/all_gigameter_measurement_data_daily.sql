
CREATE TABLE test_tables.all_gigameter_measurement_data_daily AS (
    SELECT
        school_id_giga,
        country,
        date,
        COUNT(*)            AS measurement_count,
        MAX(app_version)    AS latest_app_version
    FROM test_tables.all_gigameter_measurement_data
    WHERE day_of_week(date) BETWEEN 1 AND 5
    GROUP BY school_id_giga, country, date
);
