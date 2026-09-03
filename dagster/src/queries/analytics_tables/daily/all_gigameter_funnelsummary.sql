



-- =============================================================================
-- Script Name:     all_gigameter_funnelsummary.sql
-- Table Created:   default.all_gigameter_funnelsummary
-- Schema:          default
-- Pipeline Status: Active (Integrated: true)
--
-- Purpose:
--   Country-level summary of the GigaMeter adoption funnel and measurement
--   source distribution. Combines school registration counts, GigaMeter and
--   MLab measurement activity, connectivity status breakdowns, and regional
--   QoS provider data (Brazil NIC.BR, Mongolia LibreRouter, Kenya providers,
--   South Africa Isizwe).
--
-- Dependencies:
--   - gigamaps_production_db.public.schools (school registry)
--   - gigameter_production_db.public.school (GigaMeter registration)
--   - default.all_gmeter_only_measurements (GigaMeter measurements)
--   - default.all_mlab_only_measurements (MLab measurements)
--   - default.all_school_master (school metadata and geography)
--   - custom_dataset.country_geography (region and continent classification)
--   - qos.bra, qos.mng, qos.ken, qos.vct, qos.zaf (country QoS measurement sources)
--   - lstringer.school_connectivity_status_vw (connectivity classification --
--     see analytics-tables/views/school_connectivity_status_vw.sql)
--
-- Output Columns:  ~34 columns
-- Primary Key:     country
-- Granularity:     One row per country
--
-- Run Notes:
--   Recurring — refresh daily or weekly. Regional provider breakdowns are
--   scoped to Brazil, Mongolia, Kenya, Saint Vincent & Grenadines, and South Africa.
--
-- Last Updated:    2026-07-17 / Luke Stringer
-- ==============================================================================


  DROP TABLE IF EXISTS default.all_gigameter_funnelsummary;

  CREATE TABLE IF NOT EXISTS  default.all_gigameter_funnelsummary
  WITH (
      location = '{AZURE_BLOB_CONNECTION_URI}/warehouse/all_gigameter_funnelsummary'
  )
  AS (

WITH registered AS (
SELECT
  sm.country,
  dcs.giga_id_school AS school_id_giga,
  CAST(SUBSTRING(dcs.created, 1, 10) AS date) AS created
FROM
  gigameter_production_db.public.dailycheckapp_school dcs
LEFT JOIN
  default.all_school_master sm
ON
  dcs.giga_id_school = sm.school_id_giga
)


, gigamaps as (

  select
    DISTINCT
      sch.giga_id_school as school_id_giga,
      c.name AS country
      -- select *
  from
     gigamaps_production_db.public.schools_school  sch
  inner JOIN
    gigamaps_production_db.public.locations_country AS c
  ON
    c.id = sch.country_id
  WHERE
    sch.deleted is null
)



, gigameter as (
SELECT
  DISTINCT
    country,
    school_id_govt,
    school_id_giga
-- select *
FROM
  default.all_gmeter_only_measurements
-- no school_record_deleted filter needed here -- all_gmeter_only_measurements'
-- own school_lookup CTE already excludes deleted schools at the source
)


, mlab as (
select
  DISTINCT
    country,
    school_id_govt,
    school_id_giga
from
  default.all_mlab_only_measurements
-- no deleted filter needed here -- all_mlab_only_measurements' own school_lookup
-- CTE already excludes deleted schools at the source (and the old view's `deleted`
-- column was always NULL for MLab anyway, so this was a no-op filter)
)



-- Reads the shared connectivity classification view instead of re-deriving it here --
-- see analytics-tables/views/school_connectivity_status_vw.sql for the logic and why
-- it has a 'Disputed' bucket (this used to silently drop those rows from every count).
, connectivity_stats AS (
    SELECT
        v.gigamaps_country AS country,
        v.school_id_giga,
        v.connectivity_gigamaps
    FROM
        lstringer.school_connectivity_status_vw v
)


, connectivity as (

SELECT
  country,
  count(distinct case when connectivity_gigamaps = 'Connected' then school_id_giga end) as connected_schools,
  count(distinct case when connectivity_gigamaps = 'NotConnected' then school_id_giga end) as not_connected_schools,
  count(distinct case when connectivity_gigamaps = 'Unknown' then school_id_giga end) as unknown_connectivity_schools,
  count(distinct case when connectivity_gigamaps = 'Disputed' then school_id_giga end) as disputed_connectivity_schools
from
  connectivity_stats
GROUP BY
  country

)


, gigamaps_schools as (

SELECT
  country,
  count(distinct school_id_giga) as gigamaps_schools
from
  gigamaps
GROUP BY
  country
)



, registered_schools as (

SELECT
  country,
  min(created) as created,
  count(distinct school_id_giga) as registered_schools
from
  registered
GROUP BY
  country
)


, nic_br_schools as (
SELECT
  'Brazil' as country,
  min(date) as initial_registration_date_nicbr,
  count(distinct school_id_giga) as nicbr_schools
from
  qos.bra   --122561
GROUP BY
  1
)

, qos_mong_schools as (

SELECT
     'Mongolia' as country,
     provider,
     MIN(date) as initial_registration_date_qos_mng,
     COUNT(DISTINCT school_id_giga) qos_mng_schools
from qos.mng
where provider = 'Mongolia'
GROUP BY provider

)



------------------------
------------------------
, qos_liquidken_schools as (
SELECT
     'Kenya' as country,
     provider,
     MIN(date) as initial_registration_date_qos_liquidken,
     COUNT(DISTINCT school_id_giga) qos_liquidken_schools
from qos.ken
where provider = 'Liquid'
GROUP BY provider
)

, qos_mawinguken_schools as (
SELECT
     'Kenya' as country,
     provider,
     MIN(date) as initial_registration_date_qos_mawinguken,
     COUNT(DISTINCT school_id_giga) qos_mawinguken_schools
     -- select DISTINCT provider
from qos.ken
where provider = 'Mawingu'
GROUP BY provider
)
------------------------
------------------------


, qos_ken_schools as (
SELECT
     'Kenya' as country,
    -- provider,
     MIN(date) as initial_registration_date_qos_ken,
     COUNT(DISTINCT school_id_giga) qos_ken_schools
     -- select DISTINCT provider
from qos.ken
GROUP BY 1
)


, qos_vct_schools as (
SELECT
     'Saint Vincent and the Grenadines' as country,
     'Meraki' as provider,
     MIN(date) as initial_registration_date_qos_vct,
     COUNT(DISTINCT school_id_giga) qos_vct_schools
     -- select *
from qos.vct
GROUP BY 1,2
)

, qos_zaf_schools as (
SELECT
     'South Africa' as country,
     'Isizwe' as provider,
     MIN(date) as initial_registration_date_qos_zaf,
     COUNT(DISTINCT school_id_giga) qos_zaf_schools
from qos.zaf
GROUP BY 1,2
)



, gigameter_schools as (
SELECT
  country,
  count(distinct school_id_giga) as gigameter_schools
from
  gigameter
GROUP BY
  country
)

, mlab_schools as (
SELECT
  country,
  count(distinct school_id_govt) as mlab_schools
from
  mlab
GROUP BY
  country
)

, country_geography as (
SELECT
  DISTINCT
    g.Country,
    master.iso3_code,
    g.Code,
    g.unicef_region,
    g.itu_region,
    g.continent
    -- select *
from
  custom_dataset.country_geography g
LEFT JOIN
  default.all_school_master  master
on
  g.country = master.country
)


SELECT
  DISTINCT
  CAST(gigamaps.country AS VARCHAR) AS country,
  CAST(geo.iso3_code AS VARCHAR) AS iso3_code,
  CAST(geo.unicef_region AS VARCHAR) AS unicef_region,
  CAST(geo.itu_region AS VARCHAR) AS itu_region,
  CAST(geo.continent AS VARCHAR) AS continent,
  CAST(gigamaps.gigamaps_schools AS BIGINT) AS gigamaps_schools,
  CAST(registered.registered_schools AS BIGINT) AS registered_schools,
  CAST(registered.created AS DATE) as initial_registration_date,
  CAST(gigameter.gigameter_schools AS BIGINT) AS gigameter_schools,
  CAST(mlab.mlab_schools AS BIGINT) AS mlab_schools,
-- Brazil
  CAST(nicbr.nicbr_schools AS BIGINT) AS nicbr_schools,
  CAST(nicbr.initial_registration_date_nicbr AS DATE) AS initial_registration_date_nicbr,
-- Mongolia
  CAST(qosmng.qos_mng_schools AS BIGINT) AS qos_mng_schools,
  CAST(qosmng.initial_registration_date_qos_mng AS DATE) AS initial_registration_date_qos_mng,
-- Kenya [total]
  CAST(qosken.qos_ken_schools AS BIGINT) AS qos_ken_schools,
  CAST(qosken.initial_registration_date_qos_ken AS DATE) AS initial_registration_date_qos_ken,
-- Kenya [liquid / mawingu breakdown]
  CAST(qosliquidken.qos_liquidken_schools AS BIGINT) AS qos_liquidken_schools,
  CAST(qosliquidken.initial_registration_date_qos_liquidken AS DATE) AS initial_registration_date_qos_liquidken,
  CAST(qosmawinguken.qos_mawinguken_schools AS BIGINT) AS qos_mawinguken_schools,
  CAST(qosmawinguken.initial_registration_date_qos_mawinguken AS DATE) AS initial_registration_date_qos_mawinguken,
-- Saint Vincent and the Grenadines (VCT)
  CAST(qosvct.qos_vct_schools AS BIGINT) AS qos_vct_schools,
  CAST(qosvct.initial_registration_date_qos_vct AS DATE) AS initial_registration_date_qos_vct,
-- South Africa (ZAF)
  CAST(qoszaf.qos_zaf_schools AS BIGINT) AS qos_zaf_schools,
  CAST(qoszaf.initial_registration_date_qos_zaf AS DATE) AS initial_registration_date_qos_zaf,
-- calculated cols
  CAST(con.connected_schools AS BIGINT) AS connected_schools,

  CAST(ROUND(((con.connected_schools * DECIMAL '100.0') / NULLIF(gigamaps.gigamaps_schools, 0)), 2) AS DOUBLE) AS connected_schools_percentage,

  CAST(con.not_connected_schools AS BIGINT) AS not_connected_schools,

  CAST(ROUND(((con.not_connected_schools * DECIMAL '100.0') / NULLIF(gigamaps.gigamaps_schools, 0)), 2) AS DOUBLE) AS not_connected_schools_percentage,

  CAST(con.unknown_connectivity_schools AS BIGINT) AS unknown_connectivity_schools,

  CAST(ROUND(((con.unknown_connectivity_schools * DECIMAL '100.0') / NULLIF(gigamaps.gigamaps_schools, 0)), 2) AS DOUBLE) AS unknown_connectivity_schools_percentage,

  -- Schools where GigaMaps' connectivity_status and its own weekly connectivity
  -- boolean disagree -- previously silently excluded from every bucket above.
  -- Usually 0; only non-zero when the two signals haven't caught up with each
  -- other yet (see school_connectivity_status_vw.sql for the mechanics).
  CAST(con.disputed_connectivity_schools AS BIGINT) AS disputed_connectivity_schools,

  CAST(ROUND(((con.disputed_connectivity_schools * DECIMAL '100.0') / NULLIF(gigamaps.gigamaps_schools, 0)), 2) AS DOUBLE) AS disputed_connectivity_schools_percentage


FROM
  gigamaps_schools gigamaps
left join
  gigameter_schools gigameter
on
  gigamaps.country = gigameter.country
left join
  registered_schools registered
on
  gigamaps.country = registered.country
left join
  connectivity con
on
  gigamaps.country = con.country
left join
  mlab_schools mlab
on
  gigamaps.country = mlab.country
left join
  nic_br_schools nicbr
on
  gigamaps.country = nicbr.country
left join
  country_geography geo
on
  gigamaps.country=geo.country
left join
  qos_mong_schools  qosmng
on
   gigamaps.country = qosmng.country
left join
  qos_liquidken_schools qosliquidken
on
  gigamaps.country = qosliquidken.country
left join
  qos_mawinguken_schools qosmawinguken
on
  gigamaps.country = qosmawinguken.country
left join
  qos_ken_schools qosken
on
  gigamaps.country = qosken.country
left join
  qos_vct_schools qosvct
on
  gigamaps.country = qosvct.country
left join
  qos_zaf_schools qoszaf
on
  gigamaps.country = qoszaf.country


   )
