


 CREATE TABLE IF NOT EXISTS default.bra_nicbr_daily
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/bra_nicbr_daily'
)
as (


WITH master AS (
    SELECT DISTINCT
        COALESCE(bm.school_id_govt, fs.school_id_govt) AS school_id_govt,
        COALESCE(bm.school_id_giga, fs.school_id_giga) AS school_id_giga,
        COALESCE(bm.school_name, fs.school_name) AS school_name,
        bm.admin1,
        bm.admin2,
        bm.connectivity,
        bm.connectivity_rt,
        bm.connectivity_type_govt,
        bm.cellular_coverage_availability,
        bm.cellular_coverage_type,
        bm.school_area_type,
        bm.school_funding_type,
        bm.education_level,
        bm.fiber_node_distance,
        CASE
            WHEN bm.num_students BETWEEN 1 AND 100 THEN '1-100'
            WHEN bm.num_students BETWEEN 101 AND 200 THEN '101-200'
            WHEN bm.num_students BETWEEN 201 AND 300 THEN '201-300'
            WHEN bm.num_students > 300 THEN '300+'
            ELSE 'Unknown'
        END AS students,
        bm.num_students,
        bm.num_computers,
        COALESCE(bm.download_speed_benchmark, 50) AS download_speed_benchmark,
        fs.fust_tax_exempt,
        fs.fust_install
    FROM school_master.bra bm
    FULL JOIN default.bra_fust_schools fs
        ON bm.school_id_govt = fs.school_id_govt
),
nic_br_realtime AS (

    SELECT
        school_id_govt,
        school_id_giga,
        date,
        avg(speed_download)  as speed_download_mean,
        avg(speed_upload) as speed_upload_mean,
        avg(roundtrip_time)  AS roundtriptime_mean,
        CAST(NULL AS DOUBLE) AS jitter_download_mean,
        CAST(NULL AS DOUBLE) AS jitter_upload_mean,
        avg(rtt_packet_loss_pct) AS packetloss_pct_mean,
        count (*) as num_measurements,
        count(DISTINCT agent_id) AS num_agents
    FROM
        qos.bra
    GROUP BY
        date,
        school_id_govt,
        school_id_giga

),
vt AS (
    SELECT
        r.*,
        m.school_name,
        m.admin1,
        m.admin2,
        m.connectivity,
        m.connectivity_rt,
        m.connectivity_type_govt,
        m.school_funding_type,
        m.num_students,
        m.cellular_coverage_type,
        m.education_level,
        m.fiber_node_distance,
        m.download_speed_benchmark,
        m.students,
        COALESCE(m.fust_tax_exempt, 'FALSE') AS fust_tax_exempt,
        COALESCE(m.fust_install, 'FALSE') AS fust_install,
        CASE
            WHEN m.school_name IS NULL THEN 0
            ELSE 1
        END AS in_school_master
    FROM nic_br_realtime r
    LEFT JOIN master m
        ON r.school_id_giga = m.school_id_giga
),
final AS (
    SELECT
        *,
        CASE
            WHEN speed_download_mean >= download_speed_benchmark THEN '1 - good'
            WHEN speed_download_mean < download_speed_benchmark AND speed_download_mean > 1 THEN '2 - moderate'
            WHEN speed_download_mean < 1 THEN '3 - bad'
            ELSE 'unknown'
        END AS dl_benchmark_status,
        CASE
            WHEN speed_download_mean >= download_speed_benchmark THEN 'pass'
            WHEN speed_download_mean < download_speed_benchmark AND speed_download_mean IS NOT NULL THEN 'fail'
            ELSE 'unknown'
        END AS dl_benchmark_pass_fail,
        CASE
            WHEN num_students > 0 AND speed_download_mean / num_students < 1 THEN 'fail'
            WHEN num_students > 0 THEN 'pass'
            ELSE 'unknown'
        END AS mbps_target_pass_fail,
        CASE
            WHEN num_students > 0 THEN speed_download_mean / num_students
            ELSE NULL
        END AS mbps_per_student,
        CASE
            WHEN num_students > 0 THEN (speed_download_mean / num_students) - 1
            ELSE NULL
        END AS mbps_per_student_gap,
        speed_download_mean - download_speed_benchmark AS dl_speed_benchmark_mbps_gap,
        CASE
            WHEN download_speed_benchmark > 0 THEN CAST(speed_download_mean / download_speed_benchmark AS DECIMAL(10,5))
            ELSE NULL
        END AS dl_speed_benchmark_ratio
    FROM vt
)
SELECT
    CAST(school_id_govt AS VARCHAR) AS school_id_govt,
    CAST(school_id_giga AS VARCHAR) AS school_id_giga,
    cast(date as date) as date,
    CAST(speed_download_mean AS DOUBLE) AS speed_download_mean,
    CAST(speed_upload_mean AS DOUBLE) AS speed_upload_mean,
    CAST(roundtriptime_mean AS DOUBLE) AS roundtriptime_mean,
    CAST(jitter_download_mean AS DOUBLE) AS jitter_download_mean,
    CAST(jitter_upload_mean AS DOUBLE) AS jitter_upload_mean,
    CAST(packetloss_pct_mean AS DOUBLE) AS packetloss_pct_mean,
    CAST(num_measurements AS BIGINT) AS num_measurements,
    CAST(num_agents AS BIGINT) AS num_agents,
    CAST(school_name AS VARCHAR) AS school_name,
    CAST(admin1 AS VARCHAR) AS admin1,
    CAST(admin2 AS VARCHAR) AS admin2,
    CAST(connectivity AS VARCHAR) AS connectivity,
    CAST(connectivity_rt AS VARCHAR) AS connectivity_rt,
    CAST(connectivity_type_govt AS VARCHAR) AS connectivity_type_govt,
    CAST(school_funding_type AS VARCHAR) AS school_funding_type,
    CAST(num_students AS BIGINT) AS num_students,
    CAST(cellular_coverage_type AS VARCHAR) AS cellular_coverage_type,
    CAST(education_level AS VARCHAR) AS education_level,
    CAST(fiber_node_distance AS DOUBLE) AS fiber_node_distance,
    CAST(download_speed_benchmark AS DOUBLE) AS download_speed_benchmark,
    CAST(students AS VARCHAR) AS students,
    CAST(fust_tax_exempt AS VARCHAR) AS fust_tax_exempt,
    CAST(fust_install AS VARCHAR) AS fust_install,
    CAST(dl_benchmark_status AS VARCHAR) AS dl_benchmark_status,
    CAST(dl_benchmark_pass_fail AS VARCHAR) AS dl_benchmark_pass_fail,
    CAST(mbps_target_pass_fail AS VARCHAR) AS mbps_target_pass_fail,
    CAST(mbps_per_student AS DOUBLE) AS mbps_per_student,
    CAST(mbps_per_student_gap AS DOUBLE) AS mbps_per_student_gap,
    CAST(dl_speed_benchmark_mbps_gap AS DOUBLE) AS dl_speed_benchmark_mbps_gap,
    CAST(dl_speed_benchmark_ratio AS DOUBLE) AS dl_speed_benchmark_ratio,
    CAST(dl_benchmark_status AS VARCHAR) AS rtm_status
FROM final


 )
