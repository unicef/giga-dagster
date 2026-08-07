


CREATE TABLE IF NOT EXISTS public.superset_session_lengths as


WITH ev2 AS (
    SELECT
        l.user_id
        , u.email AS user_email
        , CASE
            WHEN lower(u.email) LIKE '%@unicef.org' THEN 'unicef'
            WHEN lower(u.email) LIKE '%@itu.%' THEN 'itu'
            WHEN lower(u.email) LIKE '%@edu%' THEN 'external'
            WHEN lower(u.email) LIKE '%.gov.%' THEN 'external'
            WHEN lower(u.email) LIKE '%thinkingmachin%' THEN 'thinkingmachine'
            WHEN lower(u.email) LIKE '%@nagarro%' THEN 'nagarro'
            ELSE 'unknown'
          END AS user_org
        , l.dttm AS log_time
        , LAG(l.dttm) OVER (PARTITION BY l.user_id ORDER BY l.dttm) AS prev_t
    FROM public.logs l
    LEFT JOIN public.ab_user u ON l.user_id = u.id
    WHERE l.user_id IS NOT NULL
)
, flagged AS (
    SELECT
        user_id
        , user_email
        , user_org
        , log_time
        , CASE
            WHEN prev_t IS NULL OR log_time - prev_t > INTERVAL '30 minutes'
            THEN 1 ELSE 0
          END AS new_session
    FROM ev2
)
, sessions AS (
    SELECT
        user_id
        , user_email
        , user_org
        , log_time
        , SUM(new_session) OVER (
            PARTITION BY user_id
            ORDER BY log_time
            ROWS UNBOUNDED PRECEDING
          ) AS session_seq
    FROM flagged
)
, spans AS (
    SELECT
        user_id
        , user_email
        , user_org
        , session_seq
        , MIN(log_time) AS session_start
        , MAX(log_time) AS session_end
        , COUNT(*) AS events
        , EXTRACT(EPOCH FROM (MAX(log_time) - MIN(log_time))) AS session_seconds
    FROM sessions
    GROUP BY 1, 2, 3, 4
)
SELECT
    user_id
    , user_email
    , user_org
    , session_start
    , session_start::date AS day
    , session_end
    , events
    , ROUND((session_seconds / 60.0)::numeric, 1) AS session_minutes
FROM spans
ORDER BY session_start;
