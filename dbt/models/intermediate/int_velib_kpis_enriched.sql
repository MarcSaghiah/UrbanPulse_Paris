{{ config(materialized='view') }}

select
  collected_at_utc,
  station_count,
  active_station_count,
  total_bikes_available,
  total_docks_available,
  total_ebikes_available,
  total_mechanical_bikes_available,
  case
    when station_count = 0 then 0
    else cast(active_station_count as double) / station_count
  end as active_station_ratio,
  case
    when total_bikes_available = 0 then 0
    else cast(total_ebikes_available as double) / total_bikes_available
  end as ebike_share,
  year,
  month,
  day,
  hour
from {{ ref('stg_velib_kpis') }}
