

-- ==============================================================================
-- Script Name:     country_versions.sql
-- Table Created:   default.country_versions
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Tracks the latest available version and date range for the school master
--   delta table for every country. Built as a UNION ALL of per-country history
--   queries, this table is consumed by all_school_master.sql to pull the most
--   recent snapshot of each country's school data.
--
-- Dependencies:
--   - delta_lake.school_master.<country_code> (one history table per country,
--     ~200+ countries; countries without a history table are marked as pending)
--
-- Output Columns:  4 (country_code, max_version, max_date, min_date)
-- Primary Key:     country_code
-- Granularity:     One row per country
--
-- Run Notes:
--   Refresh whenever delta_lake.school_master tables are updated. Countries
--   without a delta_lake history table are noted inline as "pending dataset
--   availability" and contribute NULL rows to the union.
--
-- Last Updated:    2025-10-31 / Luke Stringer
-- ==============================================================================

SET SESSION mark_distinct_strategy = 'none';
SET SESSION query_max_stage_count = 400;


CREATE TABLE IF NOT EXISTS default.country_versions
WITH (
    location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/country_versions'
)
AS (

WITH vt_union AS (



SELECT 'afg' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."afg$history"
UNION ALL
SELECT 'ago' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ago$history"
UNION ALL
SELECT 'aia' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."aia$history"
UNION ALL
SELECT 'are' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."are$history"
UNION ALL
SELECT 'arg' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."arg$history"
UNION ALL
SELECT 'arm' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."arm$history"
UNION ALL
SELECT 'atg' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."atg$history"
UNION ALL
SELECT 'aus' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."aus$history"
UNION ALL
SELECT 'aut' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."aut$history"
UNION ALL
SELECT 'aze' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."aze$history"
UNION ALL
SELECT 'bel' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."bel$history"
UNION ALL
SELECT 'ben' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ben$history"
UNION ALL
SELECT 'bfa' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."bfa$history"
UNION ALL
SELECT 'bgd' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."bgd$history"
UNION ALL
SELECT 'bgr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."bgr$history"
UNION ALL
SELECT 'bih' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."bih$history"
UNION ALL
SELECT 'blr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."blr$history"
UNION ALL
SELECT 'blz' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."blz$history"
UNION ALL
SELECT 'bol' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."bol$history"
UNION ALL
SELECT 'bra' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."bra$history"
UNION ALL
SELECT 'brb' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."brb$history"
UNION ALL
SELECT 'btn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."btn$history"
UNION ALL
SELECT 'bwa' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."bwa$history"
UNION ALL
SELECT 'caf' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."caf$history"
UNION ALL
SELECT 'can' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."can$history"
UNION ALL
SELECT 'che' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."che$history"
UNION ALL
SELECT 'chl' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."chl$history"
UNION ALL
SELECT 'chn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."chn$history"
UNION ALL
SELECT 'civ' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."civ$history"
UNION ALL
SELECT 'cmr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."cmr$history"
UNION ALL
SELECT 'cod' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."cod$history"
UNION ALL
SELECT 'cog' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."cog$history"
UNION ALL
SELECT 'col' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."col$history"
UNION ALL
SELECT 'cri' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."cri$history"
UNION ALL
SELECT 'cub' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."cub$history"
UNION ALL
SELECT 'deu' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."deu$history"
UNION ALL
SELECT 'dji' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."dji$history"
UNION ALL
SELECT 'dma' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."dma$history"
UNION ALL
SELECT 'dnk' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."dnk$history"
UNION ALL
SELECT 'dom' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."dom$history"
UNION ALL
SELECT 'dza' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."dza$history"
UNION ALL
SELECT 'ecu' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ecu$history"
UNION ALL
SELECT 'egy' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."egy$history"
UNION ALL
SELECT 'esp' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."esp$history"
UNION ALL
SELECT 'eth' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."eth$history"
UNION ALL
SELECT 'fin' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."fin$history"
UNION ALL
SELECT 'fji' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."fji$history"
UNION ALL
SELECT 'fra' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."fra$history"
UNION ALL
SELECT 'gbr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."gbr$history"
UNION ALL
SELECT 'geo' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."geo$history"
UNION ALL
SELECT 'gha' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."gha$history"
UNION ALL
SELECT 'gin' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."gin$history"
UNION ALL
SELECT 'grc' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."grc$history"
UNION ALL
SELECT 'grd' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."grd$history"
UNION ALL
SELECT 'gtm' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."gtm$history"
UNION ALL
SELECT 'hnd' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."hnd$history"
UNION ALL
SELECT 'hrv' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."hrv$history"
UNION ALL
SELECT 'hti' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."hti$history"
UNION ALL
SELECT 'hun' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."hun$history"
UNION ALL
SELECT 'idn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."idn$history"
UNION ALL
SELECT 'ind' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ind$history"
UNION ALL
SELECT 'irl' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."irl$history"
UNION ALL
SELECT 'irn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."irn$history"
UNION ALL
SELECT 'irq' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."irq$history"
UNION ALL
SELECT 'isr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."isr$history"
UNION ALL
SELECT 'ita' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ita$history"
UNION ALL
SELECT 'jam' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."jam$history"
UNION ALL
SELECT 'jpn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."jpn$history"
UNION ALL
SELECT 'kaz' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."kaz$history"
UNION ALL
SELECT 'ken' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ken$history"
UNION ALL
SELECT 'kgz' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."kgz$history"
UNION ALL
SELECT 'khm' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."khm$history"
UNION ALL
SELECT 'kir' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."kir$history"
UNION ALL
SELECT 'kna' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."kna$history"
UNION ALL
SELECT 'lbr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."lbr$history"
UNION ALL
SELECT 'lby' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."lby$history"
UNION ALL
SELECT 'lca' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."lca$history"
UNION ALL
SELECT 'lka' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."lka$history"
UNION ALL
SELECT 'lso' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."lso$history"
UNION ALL
SELECT 'ltu' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ltu$history"
UNION ALL
SELECT 'lva' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."lva$history"
UNION ALL
SELECT 'mar' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mar$history"
UNION ALL
SELECT 'mda' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mda$history"
UNION ALL
SELECT 'mdg' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mdg$history"
UNION ALL
SELECT 'mex' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mex$history"
UNION ALL
SELECT 'mli' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mli$history"
UNION ALL
SELECT 'mmr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mmr$history"
UNION ALL
SELECT 'mne' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mne$history"
UNION ALL
SELECT 'mng' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mng$history"
UNION ALL
SELECT 'moz' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."moz$history"
UNION ALL
SELECT 'mrt' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mrt$history"
UNION ALL
SELECT 'msr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."msr$history"
UNION ALL
SELECT 'mwi' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mwi$history"
UNION ALL
SELECT 'mys' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."mys$history"
UNION ALL
SELECT 'nam' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."nam$history"
UNION ALL
SELECT 'ner' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ner$history"
UNION ALL
SELECT 'nga' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."nga$history"
UNION ALL
SELECT 'nic' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."nic$history"
UNION ALL
SELECT 'nld' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."nld$history"
UNION ALL
SELECT 'nor' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."nor$history"
UNION ALL
SELECT 'npl' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."npl$history"
UNION ALL
SELECT 'nzl' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."nzl$history"
UNION ALL
SELECT 'pak' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."pak$history"
UNION ALL
SELECT 'pan' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."pan$history"
UNION ALL
SELECT 'per' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."per$history"
UNION ALL
SELECT 'phl' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."phl$history"
UNION ALL
SELECT 'png' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."png$history"
UNION ALL
SELECT 'pol' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."pol$history"
UNION ALL
SELECT 'prt' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."prt$history"
UNION ALL
SELECT 'pry' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."pry$history"
UNION ALL
SELECT 'pse' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."pse$history"
UNION ALL
SELECT 'rou' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."rou$history"
UNION ALL
SELECT 'rus' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."rus$history"
UNION ALL
SELECT 'rwa' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."rwa$history"
UNION ALL
SELECT 'sau' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."sau$history"
UNION ALL
SELECT 'sdn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."sdn$history"
UNION ALL
SELECT 'sen' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."sen$history"
UNION ALL
SELECT 'sle' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."sle$history"
UNION ALL
SELECT 'slv' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."slv$history"
UNION ALL
SELECT 'srb' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."srb$history"
UNION ALL
SELECT 'ssd' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ssd$history"
UNION ALL
SELECT 'stp' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."stp$history"
UNION ALL
SELECT 'svk' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."svk$history"
UNION ALL
SELECT 'svn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."svn$history"
UNION ALL
SELECT 'swe' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."swe$history"
UNION ALL
SELECT 'swz' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."swz$history"
UNION ALL
SELECT 'syr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."syr$history"
UNION ALL
SELECT 'tca' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."tca$history"
UNION ALL
SELECT 'tgo' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."tgo$history"
UNION ALL
SELECT 'tha' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."tha$history"
UNION ALL
SELECT 'tjk' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."tjk$history"
UNION ALL
SELECT 'tto' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."tto$history"
UNION ALL
SELECT 'tun' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."tun$history"
UNION ALL
SELECT 'tur' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."tur$history"
UNION ALL
SELECT 'twn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."twn$history"
UNION ALL
SELECT 'tza' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."tza$history"
UNION ALL
SELECT 'uga' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."uga$history"
UNION ALL
SELECT 'ukr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ukr$history"
UNION ALL
SELECT 'ury' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ury$history"
UNION ALL
SELECT 'usa' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."usa$history"
UNION ALL
SELECT 'uzb' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."uzb$history"
UNION ALL
SELECT 'vct' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."vct$history"
UNION ALL
SELECT 'ven' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."ven$history"
UNION ALL
SELECT 'vgb' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."vgb$history"
UNION ALL
SELECT 'vnm' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."vnm$history"
UNION ALL
SELECT 'zaf' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."zaf$history"
UNION ALL
SELECT 'zmb' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."zmb$history"
UNION ALL
SELECT 'zwe' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date
    FROM delta_lake.school_master."zwe$history"

-- ============================================================
-- COUNTRIES PENDING DATASET AVAILABILITY
-- Uncomment each UNION ALL block as data becomes available
-- ============================================================



-- Africa
 UNION ALL SELECT 'bdi' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."bdi$history" -- Burundi
 UNION ALL SELECT 'com' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."com$history" -- Comoros
 UNION ALL SELECT 'cpv' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."cpv$history" -- Cabo Verde
 UNION ALL SELECT 'eri' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."eri$history" -- Eritrea
 UNION ALL SELECT 'gab' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."gab$history" -- Gabon
 --UNION ALL SELECT 'gmb' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."gmb$history" -- Gambia
 --UNION ALL SELECT 'gnb' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."gnb$history" -- Guinea-Bissau
 UNION ALL SELECT 'gnq' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."gnq$history" -- Equatorial Guinea
 UNION ALL SELECT 'mus' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."mus$history" -- Mauritius
 --UNION ALL SELECT 'som' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."som$history" -- Somalia
 UNION ALL SELECT 'syc' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."syc$history" -- Seychelles
 UNION ALL SELECT 'tcd' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."tcd$history" -- Chad

-- Asia / Middle East
 UNION ALL SELECT 'bhr' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."bhr$history" -- Bahrain
 --UNION ALL SELECT 'brn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."brn$history" -- Brunei
 UNION ALL SELECT 'jor' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."jor$history" -- Jordan
 UNION ALL SELECT 'kwt' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."kwt$history" -- Kuwait
 --UNION ALL SELECT 'lao' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."lao$history" -- Laos
 --UNION ALL SELECT 'lbn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."lbn$history" -- Lebanon
 --UNION ALL SELECT 'mdv' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."mdv$history" -- Maldives
 UNION ALL SELECT 'omn' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."omn$history" -- Oman
 UNION ALL SELECT 'prk' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."prk$history" -- North Korea
 UNION ALL SELECT 'qat' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."qat$history" -- Qatar
 UNION ALL SELECT 'sgp' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."sgp$history" -- Singapore
 UNION ALL SELECT 'tkm' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."tkm$history" -- Turkmenistan
 UNION ALL SELECT 'tls' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."tls$history" -- Timor-Leste
 --UNION ALL SELECT 'yem' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."yem$history" -- Yemen

-- Europe
 UNION ALL SELECT 'alb' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."alb$history" -- Albania
 UNION ALL SELECT 'cyp' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."cyp$history" -- Cyprus
 UNION ALL SELECT 'est' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."est$history" -- Estonia
 --UNION ALL SELECT 'isl' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."isl$history" -- Iceland
 UNION ALL SELECT 'lux' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."lux$history" -- Luxembourg
 --UNION ALL SELECT 'mkd' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."mkd$history" -- North Macedonia
 UNION ALL SELECT 'mlt' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."mlt$history" -- Malta

-- Americas
 UNION ALL SELECT 'bhs' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."bhs$history" -- Bahamas
 --UNION ALL SELECT 'guy' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."guy$history" -- Guyana
 UNION ALL SELECT 'sur' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."sur$history" -- Suriname

-- Pacific / Oceania
 UNION ALL SELECT 'fsm' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."fsm$history" -- Micronesia
 --UNION ALL SELECT 'mhl' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."mhl$history" -- Marshall Islands
 UNION ALL SELECT 'nru' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."nru$history" -- Nauru
 UNION ALL SELECT 'plw' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."plw$history" -- Palau
 UNION ALL SELECT 'slb' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."slb$history" -- Solomon Islands
 UNION ALL SELECT 'ton' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."ton$history" -- Tonga
 UNION ALL SELECT 'tuv' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."tuv$history" -- Tuvalu
 UNION ALL SELECT 'vut' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."vut$history" -- Vanuatu
 UNION ALL SELECT 'wsm' AS country_code, MAX(version) AS max_version, MAX(timestamp) AS max_date, MIN(timestamp) AS min_date FROM delta_lake.school_master."wsm$history" -- Samoa

)

SELECT
  CAST(country_code AS VARCHAR) AS country_code,
  CAST(max_version AS BIGINT) AS max_version,
  CAST(max_date AS TIMESTAMP) AS max_date,
  CAST(min_date AS TIMESTAMP) AS min_date

FROM
  vt_union

 );
