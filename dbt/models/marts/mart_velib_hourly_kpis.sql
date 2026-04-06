{{ config(materialized='table') }}

select
  date_trunc('hour', collected_at_utc) as hour_ts,
  max(station_count) as station_count,
  max(active_station_count) as active_station_count,
  avg(active_station_ratio) as avg_active_station_ratio,
  avg(ebike_share) as avg_ebike_share,
  avg(total_bikes_available) as avg_bikes_available,
  avg(total_docks_available) as avg_docks_available
from {{ ref('int_velib_kpis_enriched') }}
group by 1
