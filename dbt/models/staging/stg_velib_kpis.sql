{{ config(materialized='view') }}

select
  cast(try(from_iso8601_timestamp(collected_at_utc)) as timestamp) as collected_at_utc,
  station_count,
  active_station_count,
  total_bikes_available,
  total_docks_available,
  total_ebikes_available,
  total_mechanical_bikes_available,
  year,
  month,
  day,
  hour
from {{ source('gold', 'velib_kpis_station_snapshot') }}
