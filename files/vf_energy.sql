with data_source as (
select
  datetime,
  cast(sum(w * Dry_bulb_temperature__deg__C_) / sum(w) as double precision) T_out__C_
from (
  select
    %%WEIGHT_CLAUSE%% w, datetime, Dry_bulb_temperature__deg__C_
  from
    tmy
  where
    %%WHERE_CLAUSE%%
  ) a
group by 1
),

base_data as (
select
  *,
  case when light_control > 0 then %(transp__g_m2_s_lights_on)s else %(transp__g_m2_s_lights_off)s end transp__g_m2_s_,
  (case when light_control > 0 then %(Qlights__W_m2_lights_on)s else 0 end) Qlights__W_m2_,
  greatest(
  (case when light_control > 0 then %(transp__g_m2_s_lights_on)s * %(c_heat_vapor)s else %(transp__g_m2_s_lights_off)s * %(c_heat_vapor)s end)
    + (case when light_control > 0 then %(Qlights__W_m2_lights_on)s else 0 end)
    - (greatest((case when light_control > 0 then %(day_temperature_C)s else %(night_temperature_C)s end) - T_out__C_, 0) * %(Qfacade__W_m2_multiplier)s)
  , 0) QHVAC__W_m2_
from (
  select
    *,
    case
      when %(hour_lights_on)s > %(hour_lights_off)s and (extract(hour from datetime) >= %(hour_lights_on)s or extract(hour from datetime) < %(hour_lights_off)s) then 1
      when %(hour_lights_on)s <= %(hour_lights_off)s and (extract(hour from datetime) >= %(hour_lights_on)s and extract(hour from datetime) < %(hour_lights_off)s) then 1
      else 0
    end light_control
  from
    data_source
  ) a
)

select
  round(sum(transp__g_m2_s_) * 3600 * %(total_bedspace_m2)s / 1000 * 0.264172, 2) water_transpired,
  sum(light_control) light_hours,
  sum(light_control) * %(required_ppfd)s * 3600 / 1000000 provided_light_sum,
  round(cast(0.284 *(percentile_cont(0.95) within group (order by QHVAC__W_m2_)) * %(total_bedspace_m2)s / 1000 as numeric), 3) tons_of_cooling,
  sum(Qlights__W_m2_) * %(total_bedspace_m2)s / 1000 lighting_power,
  round(cast(3.412142 * sum(QHVAC__W_m2_) * %(total_bedspace_m2)s / 1000 as numeric), 4) kbtu
from
  base_data