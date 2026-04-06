-- 1) Bikes available over time
SELECT collected_at_ts, total_bikes_available
FROM urban_pulse_paris.vw_velib_kpis
ORDER BY collected_at_ts;

-- 2) Active station ratio over time
SELECT collected_at_ts, active_station_ratio
FROM urban_pulse_paris.vw_velib_kpis
ORDER BY collected_at_ts;

-- 3) E-bike share over time
SELECT
  collected_at_ts,
  CASE
    WHEN total_bikes_available = 0 THEN 0
    ELSE CAST(total_ebikes_available AS DOUBLE) / total_bikes_available
  END AS ebike_share
FROM urban_pulse_paris.vw_velib_kpis
ORDER BY collected_at_ts;

-- 4) Latest network snapshot
SELECT
  collected_at_ts,
  station_count,
  active_station_count,
  total_bikes_available,
  total_docks_available,
  total_ebikes_available,
  total_mechanical_bikes_available
FROM urban_pulse_paris.vw_velib_kpis
ORDER BY collected_at_ts DESC
LIMIT 1;
