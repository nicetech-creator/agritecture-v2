with data_source_temperature as (
select
  datetime,
  cast(sum(w * Dry_bulb_temperature__deg__C_) / sum(w) as double precision) T_out__C_,
  cast(sum(w * Relative_Humidity____) / sum(w) as double precision) RH_out
from (
  select
    %%WEIGHT_CLAUSE_A%% w, datetime, Dry_bulb_temperature__deg__C_, Relative_Humidity____
  from
    tmy
  where
    %%WHERE_CLAUSE_A%%
) a
group by 1
),

data_source_irradiance as (
select
  datetime,
  cast(sum(w * Global_Horizontal_Irradiance__W_m2_) / sum(w) as double precision) I_glob__W_m2_
from (
  select
    %%WEIGHT_CLAUSE_B%% w, datetime, Global_Horizontal_Irradiance__W_m2_
  from
    tmy
  where
    %%WHERE_CLAUSE_B%%
) a
group by 1
),

data_source as (
select
   t.datetime, t.T_out__C_, t.RH_out, i.I_glob__W_m2_
from
  data_source_temperature t
  left join data_source_irradiance i on (t.datetime = i.datetime)
),

dli_data as (
select
  month,
  sum(1) num_days,
  cast(least(ceiling((greatest(%(dli_required)s - avg(DLI), 0) / %(dli_provided)s) * 24), 24) as numeric) supply_hours_needed
from (
  select
    extract(month from datetime) as month,
    extract(day from datetime) as day,
    sum(I_glob__W_m2_) * %(transmissivity)s * 2*3600/1000000 DLI
  from data_source
  group by 1, 2
) a
group by 1
),

/* Determine the hours for each day->night transition when the lights should be kept on */
night_start as (
select
  datetime
from (
  select
    datetime, I_glob__W_m2_,
    lag(I_glob__W_m2_) over (order by datetime) last_I_glob
  from
    data_source
) x
where I_glob__W_m2_ = 0 and last_I_glob != 0
),

/* Detect "long nights" (at least 2 days) and turn the light on at the time of the last night every day */
long_nights as (
select
  night_start, next_night_start
from (
select
  datetime night_start,
  least(lead(datetime) over (order by datetime), min(datetime) over (order by datetime) + '1 year') next_night_start
from night_start
) x where next_night_start - night_start >= '2 days'
),

long_night_starts as (
select
  long_nights.night_start + (d.extra_days * (interval '1 day')) datetime
from
  long_nights
  cross join (select * from generate_series(1, 365) as extra_days) d
where long_nights.night_start + (d.extra_days * (interval '1 day')) < long_nights.next_night_start
),

/* Collect regular night starts and artificial starts during long nights */
light_start as (
select * from night_start union all select * from long_night_starts
),

light_schedule as (
select
  extract(month from lit_time) as month,
  extract(day from lit_time) as day,
  extract(hour from lit_time) as hour
from (
  select
    start_times.datetime + (interval '1 hour') * h.extra_hours lit_time
  from (
    select
      ls.datetime, dli.supply_hours_needed
    from
      dli_data dli
      join light_start ls on (dli.month = extract(month from ls.datetime))
    where
      dli.supply_hours_needed > 0
    ) start_times
    cross join (select * from generate_series(0, 23) as extra_hours) h
  where
    start_times.supply_hours_needed > h.extra_hours
) k
),

base_data as (
select
  *,

  T_out__C_ + ((Qrad__W_m2_ + Qlights__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * %(ventilation_rate_low_wind)s * %(mean_height_m2)s / 3600)) T_in__C__low_wind,
  T_out__C_ + ((Qrad__W_m2_ + Qlights__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * %(ventilation_rate_high_wind)s * %(mean_height_m2)s / 3600)) T_in__C__high_wind,
  T_out__C_ + ((Qrad__W_m2_ + Qlights__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * %(ventilation_rate_forced_a)s * %(mean_height_m2)s / 3600)) T_in__C__forced_a,
  T_out__C_ + ((Qrad__W_m2_ + Qlights__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * %(ventilation_rate_forced_b)s * %(mean_height_m2)s / 3600)) T_in__C__forced_b

from (
  select
    datetime, T_out__C_, RH_out, I_glob__W_m2_, T_soil__C_, Rn_crop__W_m2_, is_light_on,
    (2 * %(LAI)s /((1 + %(epsilon_crop)s) * %(r_b)s + r_s_time)) * (7.4 * vpd_time + (%(epsilon_crop)s * %(r_b)s * Rn_crop__W_m2_ / (2 * %(LAI)s * %(c_heat_vapor)s))) transp__g_m2_s_,
    (2 * %(LAI)s /((1 + %(epsilon_crop)s) * %(r_b)s + r_s_time)) * (7.4 * vpd_time + (%(epsilon_crop)s * %(r_b)s * Rn_crop__W_m2_ / (2 * %(LAI)s * %(c_heat_vapor)s))) * %(c_heat_vapor)s Qplant__W_m2_,
    Qlights__W_m2_, Qrad__W_m2_,
    (T_out__C_ - T_soil__C_) * %(air_soil_transfer_coefficient)s Qsoil__W_m2_,
    case
      when (2 * %(LAI)s /((1 + %(utr)s) * %(r_b)s + r_s_time)) * (7.4 * vpd_time + (%(utr)s * %(r_b)s * Rn_crop__W_m2_ / (2 * %(LAI)s * %(c_heat_vapor)s))) * %(c_heat_vapor)s > Qrad__W_m2_ then 0
      else (2 * %(LAI)s /((1 + %(epsilon_crop)s) * %(r_b)s + r_s_time)) * (7.4 * vpd_time + (%(epsilon_crop)s * %(r_b)s * Rn_crop__W_m2_ / (2 * %(LAI)s * %(c_heat_vapor)s))) * %(c_heat_vapor)s / Qrad__W_m2_
    end Qrad_Qp,
    Qc_difT__W_m2_K_
  from (
    select
      d.datetime, d.T_out__C_, d.RH_out, d.I_glob__W_m2_,

      /* Add 2.5 is irradiance is positive, add 1.5 otherwise */
      d.T_out__C_ + case when d.I_glob__W_m2_ > 0 then 2.5 else 1.5 end T_soil__C_,

      /* Compute combined light + solar radiation */
      (
        (case when ls.month is not null then 1 else 0 end) * %(dli_provided)s * 1000000/(24*3600*5) /* light_radiation */
        + d.I_glob__W_m2_ /* solar_radiation */
      ) * %(l_s)s * (1 - exp(%(l_i)s * %(LAI)s)) * %(transmissivity)s Rn_crop__W_m2_,

      case when d.I_glob__W_m2_ > 0 then %(r_s_min)s else %(r_s_min_night)s end r_s_time,
      case when d.I_glob__W_m2_ > 0 then %(vpd_day)s else %(vpd_night)s end vpd_time,

      (case when ls.month is not null then 1 else 0 end) is_light_on,
      (case when ls.month is not null then 1 else 0 end) * (%(kW_per_sqft)s * 1000 * 10.76 - (%(dli_provided)s *1000000/(24*3600*5))) Qlights__W_m2_,

      %(transmissivity)s * (1 - %(reflectivity)s) * d.I_glob__W_m2_ + (case when ls.month is not null then 1 else 0 end) * %(dli_provided)s * 1000000/(24*3600*5)  Qrad__W_m2_,

      %(Ac_As)s * %(utr)s Qc_difT__W_m2_K_

    from
      data_source d
      left join light_schedule ls on (
        extract(hour from d.datetime) = ls.hour
        and extract(day from d.datetime) = ls.day
        and extract(month from d.datetime) = ls.month
      )
    ) a
  ) b
),

baseline_scenario as (
select
  scenario,
  scenario = 4 is_evap_cooling,
  case
    when scenario = 1 then %(ventilation_rate_low_wind)s
    when scenario = 2 then %(ventilation_rate_high_wind)s
    when scenario = 3 then %(ventilation_rate_forced_a)s
    else %(ventilation_rate_forced_b)s
  end scenario_ventilation_rate
from (
  select
    case
      when max_temperature_baseline_scenario > %(max_allowable_temp)s then 4
      else 2
    end scenario
  from (
    select
      max(case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_high_wind)s * %(mean_height_m2)s / 3600)))
      end) max_temperature_baseline_scenario
    from
      base_data
    ) baseline_temperature
 ) a
),

ventilation_data as (
select
  *,

  case
    when s.scenario = 3 or s.scenario = 4 then
      case
        when T_in__C__forced_a > %(max_allowable_temp)s then
          case 
            when Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp - Qc_difT__W_m2_K_ * ( %(max_allowable_temp)s - T_out__C_) > 0 then 
                (Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp - Qc_difT__W_m2_K_ * ( %(max_allowable_temp)s - T_out__C_)) / %(c_heat_vapor)s * (3600 / 1000)
            else 0
          end
        else 0
      end
    else 0
  end evap_cooling__kg_m2_h__forced,

  greatest(
   Qc_difT__W_m2_K_
   + (1 * %(mean_height_m2)s / 3600) * %(air_density)s * %(specific_heat_air)s * (%(min_allowable_temp)s - T_out__C_)
   - Qlights__W_m2_ - Qrad__W_m2_ + Qplant__W_m2_ * Qrad_Qp
   + (%(min_allowable_temp)s - T_soil__C_) * %(air_soil_transfer_coefficient)s
  , 0) * %(total_area_m2)s / 1000  heating_demand__kWh__closed_GH,

 (case when T_in__C__low_wind > %(max_allowable_temp)s then 1 else 0 end)
   + (case when T_in__C__forced_a > %(max_allowable_temp)s then 1 else 0 end)
   + (case when T_in__C__forced_b > %(max_allowable_temp)s then 1 else 0 end) cooling_control_low_wind,

 (case when T_in__C__high_wind > %(max_allowable_temp)s then 1 else 0 end)
   + (case when T_in__C__forced_a > %(max_allowable_temp)s then 1 else 0 end)
   + (case when T_in__C__forced_b > %(max_allowable_temp)s then 1 else 0 end) cooling_control_high_wind

from
  base_data
  cross join baseline_scenario s
),

main_data as (
select
  *,

  case
    /* 1NN */
    when scenario = 1 and not %(is_heating)s and not is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_low_wind)s * %(mean_height_m2)s / 3600)))
      end

    /* 2NN */
    when scenario = 1 and not %(is_heating)s and not is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_high_wind)s * %(mean_height_m2)s / 3600)))
      end

    /* 1YN */
    when scenario = 1 and %(is_heating)s and not is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + (((heating_demand__kWh__closed_GH * 1000 / %(total_area_m2)s) + Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_low_wind)s * %(mean_height_m2)s / 3600)))
      end

    /* 2YN */
    when scenario = 2 and %(is_heating)s and not is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + (((heating_demand__kWh__closed_GH * 1000 / %(total_area_m2)s) + Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_high_wind)s * %(mean_height_m2)s / 3600)))
      end

    /* 3NN */
    when scenario = 3 and not %(is_heating)s and not is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_forced_a)s * %(mean_height_m2)s / 3600)))
      end

    /* 4NN */
    when scenario = 4 and not %(is_heating)s and not is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_forced_b)s * %(mean_height_m2)s / 3600)))
      end

    /* 3YN */
    when scenario = 3 and %(is_heating)s and not is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + (((heating_demand__kWh__closed_GH * 1000 / %(total_area_m2)s) + Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_forced_a)s * %(mean_height_m2)s / 3600)))
      end

    /* 4YN */
    when scenario = 4 and %(is_heating)s and not is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + (((heating_demand__kWh__closed_GH * 1000 / %(total_area_m2)s)) + Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_forced_b)s * %(mean_height_m2)s / 3600)))
      end

    /* 3NY */
    when scenario = 3 and not %(is_heating)s and is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp - %(c_heat_vapor)s * (1000 / 3600) * evap_cooling__kg_m2_h__forced) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_forced_a)s * %(mean_height_m2)s / 3600)))
      end

    /* 4NY */
    when scenario = 4 and not %(is_heating)s and is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp - %(c_heat_vapor)s * (1000 / 3600) * evap_cooling__kg_m2_h__forced) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_forced_b)s * %(mean_height_m2)s / 3600)))
      end

    /* 3YY */
    when scenario = 3 and %(is_heating)s and is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + (((heating_demand__kWh__closed_GH * 1000 / %(total_area_m2)s) + Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp - %(c_heat_vapor)s * (1000 / 3600) * evap_cooling__kg_m2_h__forced) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_forced_a)s * %(mean_height_m2)s / 3600)))
      end

    /* 4YY */
    when scenario = 4 and %(is_heating)s and is_evap_cooling then
      case
        when T_in__C__low_wind < %(min_allowable_temp)s then
          T_out__C_ + (((heating_demand__kWh__closed_GH * 1000 / %(total_area_m2)s) + Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600)))
        else
          greatest(
            least(
              T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp - %(c_heat_vapor)s * (1000 / 3600) * evap_cooling__kg_m2_h__forced) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (1 * %(mean_height_m2)s / 3600))),
              %(max_allowable_temp)s
            ),
          T_out__C_ + ((Qlights__W_m2_ + Qrad__W_m2_ - Qsoil__W_m2_ - Qplant__W_m2_ * Qrad_Qp - %(c_heat_vapor)s * (1000 / 3600) * evap_cooling__kg_m2_h__forced) / (Qc_difT__W_m2_K_ + %(air_density)s * %(specific_heat_air)s * (%(ventilation_rate_forced_b)s * %(mean_height_m2)s / 3600)))
        )
      end

    /* Default */
    else
      null
  end climate_control_scenario,

  (
    1.94 * %(co2_ambient_ppm)s
    - least((is_light_on * %(optimal_avg_PPFD__umol_m2_s)s + I_glob__W_m2_ * %(transmissivity)s * 2) / %(optimal_avg_PPFD__umol_m2_s)s, 1) * %(max_unlimited_assimilation__mg_m2_s)s / (
      scenario_ventilation_rate * %(mean_height_m2)s / 3600)
  ) / 1.94 co2_in_ppm__sup_lights,

  (
    1.94 * %(co2_ambient_ppm)s
    + %(co2_injection_rate)s / (scenario_ventilation_rate * %(mean_height_m2)s / 3600)
    - least((is_light_on * %(optimal_avg_PPFD__umol_m2_s)s + I_glob__W_m2_ * %(transmissivity)s * 2) / %(optimal_avg_PPFD__umol_m2_s)s, 1) * %(max_unlimited_assimilation__mg_m2_s)s / (
      scenario_ventilation_rate * %(mean_height_m2)s / 3600)
  ) / 1.94 co2_supply__sup_lights_ppm

from
  ventilation_data
),

co2_injection_yield as (
select
  175 * (1 / reference_co2_ppm - 1 / min_co2_ppm) increase_co2_yield
from (
  select
    min(co2_supply__sup_lights_ppm) min_co2_ppm,
    min(co2_in_ppm__sup_lights) reference_co2_ppm
  from
    main_data
  ) co2_summary
),

light_temperature_yields as (
select
  case
    when %(is_lighting)s then 0
    else 1 - least((sum(gh_dli_limited) / %(light_sum)s) / %(cycles_per_year)s, 1)
  end reduction_suboptimal_light_yield,
  1 - least(sum(degree_days) / (%(cycles_per_year)s * %(degree_days_per_dev_cycle)s), 1) reduction_suboptimal_temperature
from (
  select
    extract(month from datetime) as month, extract(day from datetime) as day,
    least(sum(I_glob__W_m2_) * %(transmissivity)s * 2*3600/1000000, %(dli_required)s) gh_dli_limited,
    greatest(
      least(avg(climate_control_scenario), %(max_crop_temperature)s),
      %(min_crop_temperature)s
    ) - %(min_crop_temperature)s degree_days
  from
    main_data
group by 1, 2
) daily_summaries
),

yield_data as (
select
  lty.reduction_suboptimal_light_yield,
  lty.reduction_suboptimal_temperature,
  co2.increase_co2_yield,
  (1 + co2.increase_co2_yield) * (1 - lty.reduction_suboptimal_light_yield) * (1 - lty.reduction_suboptimal_temperature) yield_coefficient
from
  light_temperature_yields lty
  cross join co2_injection_yield co2
),

scenario_data as (
select
  scenario,
  scenario_ventilation_rate * %(total_volume_cuft)s / 60 ventilation_cfm
from
  baseline_scenario
),

heating_water_ventilation_data as (
select
  case when %(is_heating)s then sum(heating_demand__kWh__closed_GH) else 0 end total_energy_use_heating_kWh,
  case when %(is_heating)s then percentile_cont(0.965) within group (order by heating_demand__kWh__closed_GH) else 0 end heating_system_size_kW,
  sum(transp__g_m2_s_) * 3600 * %(total_area_m2)s / 1000 + sum(evap_cooling__kg_m2_h__forced) * %(total_area_m2)s water_transpired_L,
  sum(evap_cooling__kg_m2_h__forced) * %(total_area_m2)s evap_cooling_L,
  sum((case when cooling_control_low_wind = 0 then 0 else 1 end) + (case when cooling_control_high_wind = 0 then 0 else 1 end)) / 2 ventilation_hours
from main_data
)

select
  s.scenario,
  y.reduction_suboptimal_light_yield,
  y.reduction_suboptimal_temperature,
  y.increase_co2_yield,
  y.yield_coefficient,
  o.total_energy_use_heating_kWh,
  o.heating_system_size_kW,
  o.water_transpired_L,
  o.evap_cooling_L,
  s.ventilation_cfm,
  o.ventilation_hours,
  k.light_hours_per_year
from
  yield_data y
  cross join scenario_data s
  cross join heating_water_ventilation_data o
  cross join (select count(*) light_hours_per_year from light_schedule) k