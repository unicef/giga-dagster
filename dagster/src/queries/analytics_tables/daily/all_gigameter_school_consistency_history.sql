

-- SCHOOL CONSISTENCY HISTORY — BACKFILL
-- Trino/Presto Implementation
-- ============================================================================
--
-- ============================================================================
-- Purpose: Generate weekly snapshots of the 30-day rolling consistency score
--          for every school from 2024-01-01 to today, and populate the
--          school_consistency_history table.
--
-- RUN ONCE only to populate historical data. For ongoing weekly updates use:
--   school_consistency_history_weekly_append.sql
--
-- Snapshot cadence: Every Monday (2024-01-01 was a Monday; INTERVAL '7' DAY
--   from that anchor produces all subsequent Mondays automatically).
--
-- Each row represents one school's 30-day rolling score as of that Monday.
-- The 30-day window for snapshot_date = S is: [S - 30 days, S].
--
-- Performance note: This query performs a CROSS JOIN between gigameter_data
--   and the weekly anchor dates. Each measurement row matches approximately
--   4-5 weekly windows (those whose 30-day window contains that measurement
--   date). Total intermediate rows ≈ 15-20M. This is a one-time operation;
--   expect several minutes runtime on a standard Trino cluster.
--   The school_first_dates CTE performs a full table scan — if this table
--   is very large, consider pre-materialising it.
--
-- TARGET CONFIGURATION
-- ─────────────────────────────────────────────────────────────────────────────
-- Current daily test target: 4 (app version ≥ 1.0.9)
-- To revert to legacy target of 2: change all [TARGET] locations below.
-- ─────────────────────────────────────────────────────────────────────────────
--
-- ============================================================================



 DROP TABLE IF EXISTS default.all_gigameter_school_consistency_history;

 CREATE TABLE default.all_gigameter_school_consistency_history
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_school_consistency_history'
)
AS (

WITH weekly_anchors AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Generate one Monday anchor per week from backfill start to today.
    -- Each anchor becomes the window_end date for a 30-day rolling window.
    -- Adjust DATE '2024-01-01' to change the backfill start date.
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        d                                   AS snapshot_date,
        d - INTERVAL '30' DAY               AS window_start,
        d                                   AS window_end
    FROM UNNEST(SEQUENCE(
        DATE '2024-01-01',
        CURRENT_DATE,
        INTERVAL '7' DAY
    )) AS t(d)
),

school_first_dates AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- First-ever measurement date per school across the full dataset.
    -- Used to correctly size the per-school denominator for each window.
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        school_id_giga,
        MIN(date) AS first_measurement_date
    FROM default.all_gmeter_only_measurements

    GROUP BY school_id_giga
),

school_lifetime_activity AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Lifetime activity summary per school (using the same filters as the windowed
    -- stats). Used to identify true "one-shot" schools: those that measured on
    -- exactly one distinct day and never again.
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        gm.school_id_giga,
        COUNT(DISTINCT gm.date) AS lifetime_active_days,
        MIN(gm.date)            AS lifetime_first_date,
        MAX(gm.date)            AS lifetime_last_date
    FROM default.all_gigameter_measurement_data gm
    WHERE
        gm.is_weekday              = True
        AND gm.measurement_time_window = '8am-4pm(within school hrs)'
        AND gm.rt_source           = 'GigaMeter'
    GROUP BY gm.school_id_giga
),

school_window_daily_stats AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Daily measurement counts per school per weekly window.
    --
    -- The CROSS JOIN with weekly_anchors applies each window's date filter
    -- to the measurements table. Each measurement row appears in all windows
    -- whose 30-day range covers that measurement date (~4-5 windows per row).
    --
    -- Filtered to: weekdays, school hours only.
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        wa.snapshot_date,
        wa.window_start,
        wa.window_end,
        gm.school_id_giga,
        gm.school_id_govt,
        gm.school_name,
        gm.admin1,
        gm.country,
        gm.iso3_code,
        gm.date,
        COUNT(*)                                                AS daily_measurements,
        COUNT(CASE WHEN gm.notes = 'startup' THEN 1 END)       AS daily_startup,
        COUNT(CASE WHEN gm.notes = 'daily'   THEN 1 END)       AS daily_daily,
        COUNT(CASE WHEN gm.notes = 'manual'  THEN 1 END)       AS daily_manual,
        COUNT(CASE WHEN gm.notes = 'first'   THEN 1 END)       AS daily_first
    FROM default.all_gigameter_measurement_data  gm
    CROSS JOIN weekly_anchors wa
    WHERE
        gm.date                    >= wa.window_start
        AND gm.date                <= wa.window_end
        AND gm.is_weekday           = True
        AND gm.measurement_time_window = '8am-4pm(within school hrs)'
        AND gm.rt_source = 'GigaMeter'
    GROUP BY
        wa.snapshot_date, wa.window_start, wa.window_end,
        gm.school_id_giga, gm.school_id_govt, gm.school_name, gm.country, gm.iso3_code, gm.admin1, gm.date
),

active_school_windows AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Distinct (snapshot_date, school_id_giga) pairs that have any data.
    -- Used to limit the denominator UNNEST to active schools only, avoiding
    -- a full cross-product of all 22k schools × all 100+ weeks.
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT DISTINCT
        snapshot_date,
        school_id_giga
    FROM school_window_daily_stats
),

school_window_denominators AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Per-school weekday denominator for each window.
    -- Window starts from MAX(school_first_measurement_date, window_start)
    -- so recently installed schools get a proportionally smaller denominator.
    --
    -- Note: no public holiday data available — weekends only are excluded.
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        asw.snapshot_date,
        asw.school_id_giga,
        f.first_measurement_date,
        COUNT(DISTINCT d)           AS school_weekdays_in_period
    FROM active_school_windows asw
    JOIN school_first_dates f
        ON asw.school_id_giga = f.school_id_giga
    JOIN weekly_anchors wa
        ON asw.snapshot_date = wa.snapshot_date
    CROSS JOIN UNNEST(SEQUENCE(
        GREATEST(f.first_measurement_date, wa.window_start),
        wa.window_end,
        INTERVAL '1' DAY
    )) AS t(d)
    WHERE day_of_week(d) NOT IN (6, 7)   -- Exclude Sat (6) / Sun (7)
    GROUP BY
        asw.snapshot_date,
        asw.school_id_giga,
        f.first_measurement_date,
        wa.window_start,
        wa.window_end
),

school_window_scores AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Aggregate daily stats to school level and compute scoring metrics.
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        s.snapshot_date,
        s.window_start,
        s.window_end,
        s.school_id_govt,
        s.school_id_giga,
        s.school_name,
        s.country,
        s.iso3_code,
        s.admin1,

        -- Period metadata
        p.first_measurement_date                            AS school_first_date,
        p.school_weekdays_in_period,

        -- Activity metrics
        COUNT(DISTINCT s.date)                              AS active_weekdays,

        -- Days meeting the 4-test target (used for upgrade_readiness_pct)
        -- [TARGET] Change >= 4 to >= 2 for legacy app version target
        COUNT(CASE WHEN s.daily_measurements >= 4
                   THEN s.date END)                         AS days_meeting_target,

        -- Volume metrics
        SUM(s.daily_measurements)                           AS total_measurements,
        SUM(s.daily_startup)                                AS startup_count,
        SUM(s.daily_daily)                                  AS daily_count,
        SUM(s.daily_manual)                                 AS manual_count,
        SUM(s.daily_first)                                  AS first_count,

        -- Raw average (uncapped, for reference)
        ROUND(
            CAST(SUM(s.daily_measurements) AS DOUBLE)
            / NULLIF(COUNT(DISTINCT s.date), 0),
        2)                                                  AS avg_measurements_per_active_day,

        -- Partial credit quality score (see v2 README for formula detail)
        -- [TARGET] Change both 4 values to 2 for legacy target
        ROUND(
            AVG(CAST(LEAST(s.daily_measurements, 4) AS DOUBLE) / 4.0) * 100,
        1)                                                  AS avg_daily_quality_pct

    FROM school_window_daily_stats s
    JOIN school_window_denominators p
        ON  s.snapshot_date   = p.snapshot_date
        AND s.school_id_giga  = p.school_id_giga
    GROUP BY
        s.snapshot_date, s.window_start, s.window_end,
        s.school_id_giga, s.school_id_govt, s.school_name, s.country, s.iso3_code, s.admin1,
        p.first_measurement_date, p.school_weekdays_in_period
),

scored_base AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Compute derived rates and flags.
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        sws.snapshot_date,
        sws.window_start,
        sws.window_end,
        sws.school_id_giga,
        sws.school_id_govt,
        sws.school_name,
        sws.country,
        sws.iso3_code,
        sws.admin1,
        sws.school_first_date,
        sws.school_weekdays_in_period,
        sws.active_weekdays,
        sws.days_meeting_target,
        sws.total_measurements,
        sws.startup_count,
        sws.daily_count,
        sws.manual_count,
        sws.first_count,
        sws.avg_measurements_per_active_day,

        -- Coverage rate
        ROUND(
            CAST(sws.active_weekdays AS DOUBLE)
            / NULLIF(sws.school_weekdays_in_period, 0) * 100,
        1)                                                  AS coverage_rate_pct,

        sws.avg_daily_quality_pct,

        -- Upgrade readiness: % of active days meeting 4-test target
        -- [TARGET] Change >= 4 to >= 2 for legacy target
        ROUND(
            CAST(sws.days_meeting_target AS DOUBLE)
            / NULLIF(sws.active_weekdays, 0) * 100,
        1)                                                  AS upgrade_readiness_pct,

        -- Manual engagement
        CASE
            WHEN sws.manual_count > 10 THEN 'High'
            WHEN sws.manual_count >= 3 THEN 'Moderate'
            WHEN sws.manual_count >= 1 THEN 'Low'
            ELSE 'None'
        END                                                 AS manual_engagement_level,

        -- Data sufficiency flag
        CASE WHEN sws.active_weekdays < 5 THEN FALSE ELSE TRUE END  AS data_sufficiency_flag,

        -- One-shot flag
        CASE
            WHEN la.lifetime_active_days = 1 THEN TRUE
            ELSE FALSE
        END                                                 AS is_one_shot

    FROM school_window_scores sws
    LEFT JOIN school_lifetime_activity la
        ON sws.school_id_giga = la.school_id_giga
),

scored AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Compute scores and classifications (using data_sufficiency_flag for clarity).
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        sb.*,

        -- Consistency score (NULL if insufficient data)
        CASE
            WHEN sb.data_sufficiency_flag = FALSE THEN NULL
            ELSE ROUND(
                (CAST(sb.active_weekdays AS DOUBLE) / NULLIF(sb.school_weekdays_in_period, 0))
                * (sb.avg_daily_quality_pct / 100.0) * 100,
            1)
        END                                                 AS consistency_score,

        -- Classification
        CASE
            WHEN sb.data_sufficiency_flag = FALSE THEN NULL
            WHEN (CAST(sb.active_weekdays AS DOUBLE) / NULLIF(sb.school_weekdays_in_period, 0))
                 * (sb.avg_daily_quality_pct / 100.0) * 100 >= 80 THEN 'Excellent'
            WHEN (CAST(sb.active_weekdays AS DOUBLE) / NULLIF(sb.school_weekdays_in_period, 0))
                 * (sb.avg_daily_quality_pct / 100.0) * 100 >= 60 THEN 'Good'
            WHEN (CAST(sb.active_weekdays AS DOUBLE) / NULLIF(sb.school_weekdays_in_period, 0))
                 * (sb.avg_daily_quality_pct / 100.0) * 100 >= 40 THEN 'Moderate'
            WHEN (CAST(sb.active_weekdays AS DOUBLE) / NULLIF(sb.school_weekdays_in_period, 0))
                 * (sb.avg_daily_quality_pct / 100.0) * 100 >= 20 THEN 'Low'
            ELSE 'Minimal'
        END                                                 AS consistency_classification,

        -- Numeric rank for classification (used for deterioration comparison)
        CASE
            WHEN sb.data_sufficiency_flag = FALSE THEN 0
            WHEN (CAST(sb.active_weekdays AS DOUBLE) / NULLIF(sb.school_weekdays_in_period, 0))
                 * (sb.avg_daily_quality_pct / 100.0) * 100 >= 80 THEN 5
            WHEN (CAST(sb.active_weekdays AS DOUBLE) / NULLIF(sb.school_weekdays_in_period, 0))
                 * (sb.avg_daily_quality_pct / 100.0) * 100 >= 60 THEN 4
            WHEN (CAST(sb.active_weekdays AS DOUBLE) / NULLIF(sb.school_weekdays_in_period, 0))
                 * (sb.avg_daily_quality_pct / 100.0) * 100 >= 40 THEN 3
            WHEN (CAST(sb.active_weekdays AS DOUBLE) / NULLIF(sb.school_weekdays_in_period, 0))
                 * (sb.avg_daily_quality_pct / 100.0) * 100 >= 20 THEN 2
            ELSE 1
        END                                                 AS classification_rank

    FROM scored_base sb
),

scored_with_ranks AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Add within-country rank per snapshot.
    -- NULL for schools with insufficient data (data_sufficiency_flag = FALSE).
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        *,
        CASE
            WHEN data_sufficiency_flag = FALSE THEN NULL
            ELSE ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, iso3_code
                ORDER BY consistency_score DESC NULLS LAST
            )
        END                                                 AS country_rank
    FROM scored
),

scored_with_trends AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Add week-on-week trend columns using LAG().
    -- prev_* columns reference the previous weekly snapshot for the same school.
    -- NULL for a school's first appearance in the history.
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        *,
        LAG(snapshot_date)              OVER w AS prev_snapshot_date,
        LAG(consistency_score)          OVER w AS prev_consistency_score,
        LAG(consistency_classification) OVER w AS prev_consistency_classification,
        LAG(classification_rank)        OVER w AS prev_classification_rank,
        LAG(country_rank)               OVER w AS prev_country_rank
    FROM scored_with_ranks
    WINDOW w AS (
        PARTITION BY school_id_giga
        ORDER BY snapshot_date
    )
),

scored_final AS (
    -- ─────────────────────────────────────────────────────────────────────────
    -- Compute score_delta, country_rank_delta and deterioration_flag here so
    -- they are available as named columns for the mover window functions below.
    -- ─────────────────────────────────────────────────────────────────────────
    SELECT
        *,

        -- Score delta (positive = improvement, negative = decline)
        -- NULL if either this week or previous week has no score
        CASE
            WHEN consistency_score IS NULL OR prev_consistency_score IS NULL THEN NULL
            ELSE ROUND(consistency_score - prev_consistency_score, 1)
        END                                                 AS score_delta,

        -- Country rank delta (positive = moved up leaderboard, negative = moved down)
        -- NULL if either week has no rank
        CASE
            WHEN country_rank IS NULL OR prev_country_rank IS NULL THEN NULL
            ELSE prev_country_rank - country_rank
        END                                                 AS country_rank_delta,

        -- Deterioration flag
        -- TRUE if: score dropped 10+ points  OR  classification band downgraded
        CASE
            WHEN consistency_score IS NULL OR prev_consistency_score IS NULL THEN FALSE
            WHEN ROUND(consistency_score - prev_consistency_score, 1) <= -10  THEN TRUE
            WHEN classification_rank < prev_classification_rank               THEN TRUE
            ELSE FALSE
        END                                                 AS deterioration_flag,

        -- Human-readable explanation of which condition(s) triggered the flag.
        -- NULL when neither condition is met.
        CASE
            WHEN consistency_score IS NULL OR prev_consistency_score IS NULL
                THEN NULL
            WHEN ROUND(consistency_score - prev_consistency_score, 1) <= -10
                 AND classification_rank < prev_classification_rank
                THEN 'Score drop >=10pts; Band downgrade'
            WHEN ROUND(consistency_score - prev_consistency_score, 1) <= -10
                THEN 'Score drop >=10pts'
            WHEN classification_rank < prev_classification_rank
                THEN 'Band downgrade'
            ELSE NULL
        END                                                 AS deterioration_reason,
        CASE
            WHEN consistency_score IS NULL OR prev_consistency_score IS NULL
                THEN NULL
            WHEN ROUND(consistency_score - prev_consistency_score, 1) <= -10
                 AND classification_rank < prev_classification_rank
                THEN 'both'
            WHEN ROUND(consistency_score - prev_consistency_score, 1) <= -10
                THEN 'Score drop'
            WHEN classification_rank < prev_classification_rank
                THEN 'Band drop'
            ELSE NULL
        END                                                 AS deterioration_reason_short

    FROM scored_with_trends
)

-- ── Final SELECT ──────────────────────────────────────────────────────────────
SELECT
    *,

    -- Biggest weekly gainer per country:
    -- TRUE for the school with the highest positive score_delta in its country
    -- this snapshot week. FALSE if score_delta is NULL or not positive.
    CAST(CASE
        WHEN score_delta IS NULL OR score_delta <= 0 THEN FALSE
        WHEN RANK() OVER (
            PARTITION BY snapshot_date, iso3_code
            ORDER BY score_delta DESC NULLS LAST
        ) = 1 THEN TRUE
        ELSE FALSE
    END AS BOOLEAN)                                         AS is_biggest_weekly_gainer,

    -- Biggest weekly decliner per country:
    -- TRUE for the school with the most negative score_delta in its country
    -- this snapshot week. FALSE if score_delta is NULL or not negative.
    CAST(CASE
        WHEN score_delta IS NULL OR score_delta >= 0 THEN FALSE
        WHEN RANK() OVER (
            PARTITION BY snapshot_date, iso3_code
            ORDER BY score_delta ASC NULLS LAST
        ) = 1 THEN TRUE
        ELSE FALSE
    END AS BOOLEAN)                                         AS is_biggest_weekly_decliner

FROM scored_final
ORDER BY
    snapshot_date,
    iso3_code,
    consistency_score DESC NULLS LAST

  )
