


DROP TABLE IF EXISTS default.all_gigameter_school_percentile_distributions ;

CREATE TABLE default.all_gigameter_school_percentile_distributions

WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_school_percentile_distributions',
    partitioned_by = ARRAY['country']
)
AS (

SELECT
        school_id_govt
      , MIN(school_id_giga) AS school_id_giga
      , MIN(school_name) AS school_name
      , MIN(admin1) AS admin1
      , MIN(admin2) AS admin2
      , MIN(iso3_code) AS iso3_code
      , MIN(country) AS country
      , MIN(school_area_type) AS school_area_type
      , MIN(education_level) AS education_level
      , MIN(connectivity_type) AS connectivity_type
      , MIN(isp_name) AS isp_name
      , MIN(latitude) AS latitude
      , MIN(longitude) AS longitude
      , COUNT(*) AS sample_count
        -- metric percentiles (latency/loss labels inverted: p95 = best)
      , approx_percentile(download_speed, 0.25) AS download_p25
      , approx_percentile(download_speed, 0.5) AS download_p50
      , approx_percentile(download_speed, 0.75) AS download_p75
      , approx_percentile(download_speed, 0.9) AS download_p90
      , approx_percentile(download_speed, 0.95) AS download_p95
      , approx_percentile(upload_speed, 0.25) AS upload_p25
      , approx_percentile(upload_speed, 0.5) AS upload_p50
      , approx_percentile(upload_speed, 0.75) AS upload_p75
      , approx_percentile(upload_speed, 0.9) AS upload_p90
      , approx_percentile(upload_speed, 0.95) AS upload_p95
      , approx_percentile(latency, 0.75) AS latency_p25
      , approx_percentile(latency, 0.5) AS latency_p50
      , approx_percentile(latency, 0.25) AS latency_p75
      , approx_percentile(latency, 0.1) AS latency_p90
      , approx_percentile(latency, 0.05) AS latency_p95
      , approx_percentile(packet_loss_rate, 0.75) AS loss_p25
      , approx_percentile(packet_loss_rate, 0.5) AS loss_p50
      , approx_percentile(packet_loss_rate, 0.25) AS loss_p75
      , approx_percentile(packet_loss_rate, 0.1) AS loss_p90
      , approx_percentile(packet_loss_rate, 0.05) AS loss_p95
    FROM default.all_gigameter_measurement_data
    WHERE measurement_time_window = '8am-4pm(within school hrs)'
    GROUP BY school_id_govt

)

;
