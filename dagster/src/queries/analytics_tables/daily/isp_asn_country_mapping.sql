-- ==============================================================================
-- Script Name:     isp_asn_country_mapping.sql
-- Table Created:   default.isp_asn_country_mapping
-- Schema:          default
-- Pipeline Status: Active
--
-- Purpose:
--   Canonical ISP name/ASN lookup, one row per (country, raw ISP name, raw
--   ASN) ever observed across GigaMeter and MLab measurements. Resolves
--   ASN-truncation variants (e.g. a name reported under both a full ASN and
--   a truncated prefix of it) to a single canonical ASN and display name per
--   (country, ASN), and flags ASNs seen across >=3 countries as likely
--   shared infrastructure (CDN/hosting) rather than a genuine local ISP.
--   Joined back onto default.all_gigameter_measurement_data to produce
--   isp_name_clean / isp_asn_clean / isp_key / is_likely_shared_infra.
--
-- Dependencies:
--   - default.all_gmeter_only_measurements (GigaMeter raw ISP/ASN strings)
--   - default.all_mlab_only_measurements (MLab raw ISP/ASN strings)
--
-- Output Columns:  13 columns
-- Primary Key:     iso3_code + isp_name_raw + isp_asn_raw
-- Granularity:     One row per (country, raw ISP name, raw ASN) combination
--                  ever observed in either measurement source
--
-- CAVEATS:
--   - ASN-truncation clustering resolves at most 2 hops deep (hop1 + one
--     resolution pass) -- deliberately not a full recursive union-find,
--     which blew Trino's stage limit at this CTE depth. No observed
--     real-world case needs more than 2 hops.
--   - Neighbour resolution is weighted by row_count, not string length --
--     a longer ASN string doesn't always win (e.g. a Fiji case where
--     AS14147 has far more rows than AS141470 for the same ISP).
--   - name_display_norm's legal-suffix stripping list is deliberately
--     generic (ltd/llc/inc/gmbh/... legal-form tokens only), not
--     brand/company-specific -- extend it over time as needed.
--   - is_likely_shared_infra is a heuristic (canonical_asn seen in >=3
--     countries), not a maintained provider allowlist/blocklist.
--   - built_at reflects when this table was last rebuilt, not when a given
--     ISP/ASN combination was first observed.
--
-- Last Updated:    2026-07-30 / Luke Stringer
-- ==============================================================================

DROP TABLE IF EXISTS default.isp_asn_country_mapping;

CREATE TABLE default.isp_asn_country_mapping
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/isp_asn_country_mapping'
)
AS (

-- -----------------------------------------------------------------------
-- CTE: raw_combos / combined
-- Union step 1 + step 2, pre-aggregated to (country, raw name, raw asn).
-- -----------------------------------------------------------------------
WITH raw_combos AS (
    SELECT iso3_code, MAX(country) AS country,
           detected_isp_raw AS isp_name_raw, detected_isp_asn AS isp_asn_raw,
           COUNT(*) AS row_count
    FROM default.all_gmeter_only_measurements
    GROUP BY iso3_code, detected_isp_raw, detected_isp_asn
    UNION ALL
    SELECT iso3_code, MAX(country) AS country,
           detected_isp_raw AS isp_name_raw, detected_isp_asn AS isp_asn_raw,
           COUNT(*) AS row_count
    FROM default.all_mlab_only_measurements
    GROUP BY iso3_code, detected_isp_raw, detected_isp_asn
),
combined AS (
    SELECT iso3_code, MAX(country) AS country, isp_name_raw, isp_asn_raw,
           SUM(row_count) AS row_count
    FROM raw_combos
    GROUP BY iso3_code, isp_name_raw, isp_asn_raw
),

-- -----------------------------------------------------------------------
-- CTE: norm
-- Generic normalization: strip embedded AS<digits> tokens from the name
-- FIRST - some raw names embed the ASN, some don't, and skipping this
-- step silently splits what should be one group - then extract a clean
-- AS<digits> form for the ASN column.
-- -----------------------------------------------------------------------
norm AS (
    SELECT
        iso3_code, country, isp_name_raw, isp_asn_raw, row_count,
        REGEXP_REPLACE(isp_name_raw, '(?i)AS\d{2,}', '') AS name_no_asn,
        CASE WHEN REGEXP_EXTRACT(isp_asn_raw, '(\d+)', 1) IS NOT NULL
             THEN 'AS' || REGEXP_EXTRACT(isp_asn_raw, '(\d+)', 1)
             ELSE NULL END AS asn_norm
    FROM combined
),

-- -----------------------------------------------------------------------
-- CTE: norm2
-- "Light" name normalization (case/quotes/whitespace only - NO legal-suffix
-- stripping here). This is the grouping key for ASN clustering, matching
-- the verified pattern of IDENTICAL name text across truncated ASN values.
-- -----------------------------------------------------------------------
norm2 AS (
    SELECT iso3_code, country, isp_name_raw, isp_asn_raw, asn_norm, row_count,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(
            LOWER(REPLACE(REPLACE(name_no_asn, '"', ''), '''', '')),
            '[^a-z0-9]+', ' '), '\s+', ' ')) AS name_norm_light
    FROM norm
),

-- -----------------------------------------------------------------------
-- STAGE 1: ASN truncation clustering within (country, name_norm_light)
-- -----------------------------------------------------------------------
nodes AS (
    SELECT iso3_code, name_norm_light, asn_norm, SUM(row_count) AS total_rows
    FROM norm2
    WHERE asn_norm IS NOT NULL AND name_norm_light <> ''
    GROUP BY iso3_code, name_norm_light, asn_norm
),
edges AS (
    -- a is a strict prefix of b, both non-null ASNs sharing the same
    -- (country, name_norm_light) group
    SELECT a.iso3_code, a.name_norm_light,
           a.asn_norm AS asn_a, b.asn_norm AS asn_b,
           a.total_rows AS rows_a, b.total_rows AS rows_b
    FROM nodes a
    JOIN nodes b
      ON a.iso3_code = b.iso3_code
     AND a.name_norm_light = b.name_norm_light
     AND a.asn_norm <> b.asn_norm
     AND LENGTH(a.asn_norm) < LENGTH(b.asn_norm)
     AND b.asn_norm LIKE a.asn_norm || '%'
),
edges_undirected AS (
    SELECT iso3_code, name_norm_light, asn_a AS asn_norm, asn_b AS nbr_asn, rows_b AS nbr_rows FROM edges
    UNION ALL
    SELECT iso3_code, name_norm_light, asn_b AS asn_norm, asn_a AS nbr_asn, rows_a AS nbr_rows FROM edges
),
best_neighbor AS (
    SELECT iso3_code, name_norm_light, asn_norm, nbr_asn, nbr_rows,
        ROW_NUMBER() OVER (
            PARTITION BY iso3_code, name_norm_light, asn_norm
            ORDER BY nbr_rows DESC, nbr_asn
        ) AS rn
    FROM edges_undirected
),
-- Pass 1: each node points to its highest-row-count prefix-neighbour,
-- but ONLY if that neighbour actually has more rows (weighted-by-count,
-- not "longer string always wins" - a real Fiji counter-example,
-- AS14147 (16 rows) vs AS141470 (2 rows) for the same ISP, proves this
-- matters - see header CAVEATS).
hop1 AS (
    SELECT n.iso3_code, n.name_norm_light, n.asn_norm, n.total_rows,
        CASE WHEN bn.nbr_rows > n.total_rows THEN bn.nbr_asn ELSE n.asn_norm END AS target_asn
    FROM nodes n
    LEFT JOIN best_neighbor bn
      ON bn.iso3_code = n.iso3_code AND bn.name_norm_light = n.name_norm_light
     AND bn.asn_norm = n.asn_norm AND bn.rn = 1
),
-- Pass 2: resolve one further hop (safety net for a theoretical 2-link
-- chain; not needed by any observed real-world case, deliberately NOT
-- using WITH RECURSIVE - a full recursive union-find blew Trino's stage
-- limit at this CTE depth - see header CAVEATS).
asn_resolved AS (
    SELECT h1.iso3_code, h1.name_norm_light, h1.asn_norm,
        COALESCE(h1b.target_asn, h1.target_asn) AS canonical_asn
    FROM hop1 h1
    LEFT JOIN hop1 h1b
      ON h1b.iso3_code = h1.iso3_code AND h1b.name_norm_light = h1.name_norm_light
     AND h1b.asn_norm = h1.target_asn
),

-- -----------------------------------------------------------------------
-- Attach resolved canonical_asn back onto every raw row (grain-preserving)
-- -----------------------------------------------------------------------
rows_with_canon_asn AS (
    SELECT n2.iso3_code, n2.country, n2.isp_name_raw, n2.isp_asn_raw,
           n2.row_count, n2.name_norm_light, n2.asn_norm,
           COALESCE(ar.canonical_asn, n2.asn_norm) AS canonical_asn
    FROM norm2 n2
    LEFT JOIN asn_resolved ar
      ON ar.iso3_code = n2.iso3_code AND ar.name_norm_light = n2.name_norm_light
     AND ar.asn_norm = n2.asn_norm
),

-- -----------------------------------------------------------------------
-- STAGE 2: pick a single canonical display name per (country, canonical_asn)
-- "Display" normalization adds generic legal-entity-suffix stripping.
-- Suffix list is deliberately generic (legal-form tokens only) - NOT
-- brand/company-name-specific. Extend this list over time as needed;
-- it never needs a specific ISP or country name added to it.
-- -----------------------------------------------------------------------
name_display_norm AS (
    SELECT *,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(name_norm_light,
            '(?i)\b(ltd|limited|llc|lcc|inc|incorporated|corp|corporation|plc|' ||
            'gmbh|jsc|cjsc|ojsc|pjsc|pte|pty|sa|sas|ag|srl|bv|oy|ooo|doo|' ||
            'd o o|d d|a d|b v|s a|s a s|' ||
            'joint stock company|closed joint stock company|' ||
            'open joint stock company|limited liability company)\b',
            ''), '\s+', ' ')) AS name_norm_display
    FROM rows_with_canon_asn
),
name_pick AS (
    SELECT iso3_code, canonical_asn, name_norm_display, SUM(row_count) AS variant_rows
    FROM name_display_norm
    WHERE canonical_asn IS NOT NULL
    GROUP BY iso3_code, canonical_asn, name_norm_display
),
canonical_names AS (
    SELECT iso3_code, canonical_asn, name_norm_display AS canonical_isp_name,
        ROW_NUMBER() OVER (
            PARTITION BY iso3_code, canonical_asn
            ORDER BY variant_rows DESC, name_norm_display
        ) AS rn
    FROM name_pick
),
canonical_names_final AS (
    SELECT iso3_code, canonical_asn, canonical_isp_name
    FROM canonical_names
    WHERE rn = 1
),

-- -----------------------------------------------------------------------
-- STAGE 3: shared-infrastructure signal, computed GLOBALLY (not per
-- country) - distinct country count per resolved canonical_asn.
-- Fully dynamic; no hardcoded hosting/CDN provider list.
-- -----------------------------------------------------------------------
asn_country_spread AS (
    SELECT canonical_asn, COUNT(DISTINCT iso3_code) AS asn_distinct_country_count
    FROM rows_with_canon_asn
    WHERE canonical_asn IS NOT NULL
    GROUP BY canonical_asn
)

-- -----------------------------------------------------------------------
-- FINAL SELECT: one row per (country, isp_name_raw, isp_asn_raw) ever
-- observed, enriched with resolved canonical columns.
-- -----------------------------------------------------------------------
SELECT
    r.iso3_code,
    r.country,
    r.isp_name_raw,
    r.isp_asn_raw,
    r.row_count,
    r.asn_norm,
    r.canonical_asn,
    COALESCE(cn.canonical_isp_name, r.name_norm_light, '(unknown)') AS canonical_isp_name,
    TO_HEX(MD5(TO_UTF8(
        r.iso3_code || '|' ||
        COALESCE(cn.canonical_isp_name, r.name_norm_light, '(unknown)') || '|' ||
        COALESCE(r.canonical_asn, 'NULL')
    ))) AS isp_key,
    (r.asn_norm IS NOT NULL AND r.canonical_asn IS NOT NULL
        AND r.asn_norm <> r.canonical_asn) AS is_asn_truncation_merge,
    COALESCE(acs.asn_distinct_country_count, 0) AS asn_distinct_country_count,
    (COALESCE(acs.asn_distinct_country_count, 0) >= 3) AS is_likely_shared_infra,
    CURRENT_TIMESTAMP AS built_at
FROM rows_with_canon_asn r
LEFT JOIN canonical_names_final cn
  ON cn.iso3_code = r.iso3_code AND cn.canonical_asn = r.canonical_asn
LEFT JOIN asn_country_spread acs
  ON acs.canonical_asn = r.canonical_asn
);
