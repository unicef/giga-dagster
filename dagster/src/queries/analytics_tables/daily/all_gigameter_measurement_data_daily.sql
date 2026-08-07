
CREATE TABLE default.all_gigameter_measurement_data_daily
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_measurement_data_daily'
)
AS (
    SELECT
        school_id_giga,
        country,
        date,
        COUNT(*)            AS measurement_count,
        MAX(app_version)    AS latest_app_version
    FROM default.all_gigameter_measurement_data
    WHERE day_of_week(date) BETWEEN 1 AND 5
    GROUP BY school_id_giga, country, date
);
