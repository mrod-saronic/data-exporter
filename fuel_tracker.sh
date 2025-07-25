#!/bin/bash

SCALE=1000
total_ml=0
last_ms=0
last_hour_index=0
ml_at_last_hour=0
latest_hourly_ml=0

submsg | grep -i EngineFuelEconomy | while read -r line; do
  rate_str="${line##*\"engine_fuel_rate\":}"
  rate_str="${rate_str%%}*}"
  rate_str="${rate_str%%[^0-9.]*}"

  rate_raw=${rate_str/./}
  decimal_places=$(echo "$rate_str" | cut -d'.' -f2 | wc -c)
  decimal_places=$((decimal_places - 1))
  while [ "$decimal_places" -lt 6 ]; do
    rate_raw="${rate_raw}0"
    decimal_places=$((decimal_places + 1))
  done
  rate_raw=$((10#$rate_raw))

  rate_mls=$((rate_raw / 3600))

  now_sec=$(date +%s)
  now_ms=$(date +%3N | sed 's/^0*//')
  now_ms=${now_ms:-0}
  now_total_ms=$((now_sec * 1000 + 10#$now_ms))

  if [ "$last_ms" -eq 0 ]; then
    last_ms=$now_total_ms
    last_hour_index=$((now_sec / 3600))
    continue
  fi

  dt_ms=$((now_total_ms - last_ms))
  [ "$dt_ms" -lt 0 ] && last_ms=$now_total_ms && continue
  last_ms=$now_total_ms

  delta_ml=$((rate_mls * dt_ms / 1000000))
  total_ml=$((total_ml + delta_ml))

  current_hour_index=$((now_sec / 3600))
  if [ "$current_hour_index" -gt "$last_hour_index" ]; then
    latest_hourly_ml=$((total_ml - ml_at_last_hour))
    ml_at_last_hour=$total_ml
    last_hour_index=$current_hour_index
  fi

  t_ml=$((10#$total_ml))
  h_ml=$((10#$latest_hourly_ml))

  t_l_int=$((t_ml / 1000))
  t_l_frac=$(((t_ml % 1000) / 10))
  h_l_int=$((h_ml / 1000))
  h_l_frac=$(((h_ml % 1000) / 10))

  t_gal=$((t_ml * 264 / 1000000))
  t_gal_frac=$(((t_ml * 264 / 1000) % 1000 / 10))
  h_gal=$((h_ml * 264 / 1000000))
  h_gal_frac=$(((h_ml * 264 / 1000) % 1000 / 10))

  echo ""
  echo "==== Fuel Report ===="
  echo "Flow Rate:      $rate_str L/h"
  echo "Total:          $t_l_int.$t_l_frac L | $t_gal.$t_gal_frac gal"
  echo "Last Hour:      $h_l_int.$h_l_frac L | $h_gal.$h_gal_frac gal"
  echo "Time:           $(date)"
  echo "====================="
done