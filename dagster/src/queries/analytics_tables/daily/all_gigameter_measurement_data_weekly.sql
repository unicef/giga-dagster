
CREATE TABLE test_tables.all_gigameter_measurement_data_weekly AS (
    SELECT
        DATE_TRUNC('week', date)                                    AS week_start,
        school_id_giga,
        country,
        admin1,
        admin2,
        -- Most recent ISP seen that week (avoids subquery join in every chart)
        MAX_BY(isp_name, date)                                      AS isp_name,
        COUNT(*)                                                     AS measurement_count,

        -- Download speed (Mbps) — p5 / median / p95
        ROUND(APPROX_PERCENTILE(download_speed, 0.05), 2)           AS p5_download_mbps,
        ROUND(APPROX_PERCENTILE(download_speed, 0.50), 2)           AS p50_download_mbps,
        ROUND(APPROX_PERCENTILE(download_speed, 0.95), 2)           AS p95_download_mbps,

        -- Upload speed (Mbps) — p5 / median / p95
        ROUND(APPROX_PERCENTILE(upload_speed, 0.05), 2)             AS p5_upload_mbps,
        ROUND(APPROX_PERCENTILE(upload_speed, 0.50), 2)             AS p50_upload_mbps,
        ROUND(APPROX_PERCENTILE(upload_speed, 0.95), 2)             AS p95_upload_mbps,

        -- Latency (ms) — p5 / median / p95
        ROUND(APPROX_PERCENTILE(CAST(latency AS DOUBLE), 0.05), 1)  AS p5_latency_ms,
        ROUND(APPROX_PERCENTILE(CAST(latency AS DOUBLE), 0.50), 1)  AS p50_latency_ms,
        ROUND(APPROX_PERCENTILE(CAST(latency AS DOUBLE), 0.95), 1)  AS p95_latency_ms,

        -- Packet loss (rate 0–1, NOT percentage — charts multiply ×100)
        ROUND(APPROX_PERCENTILE(packet_loss_rate, 0.05), 6)         AS p5_packet_loss_rate,
        ROUND(APPROX_PERCENTILE(packet_loss_rate, 0.50), 6)         AS p50_packet_loss_rate,
        ROUND(APPROX_PERCENTILE(packet_loss_rate, 0.95), 6)         AS p95_packet_loss_rate

    FROM test_tables.all_gigameter_measurement_data
    WHERE day_of_week(date) BETWEEN 1 AND 5   -- working days only, consistent with all dashboard charts
    GROUP BY
        DATE_TRUNC('week', date),
        school_id_giga,
        country,
        admin1,
        admin2
);
