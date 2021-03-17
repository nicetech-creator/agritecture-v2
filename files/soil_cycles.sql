with data_source_temperature as (
select
  datetime,
  cast(sum(w * Dry_bulb_temperature__deg__C_) / sum(w) as double precision) T_out__C_
from (
  select
    %%WEIGHT_CLAUSE_A%% w, datetime, Dry_bulb_temperature__deg__C_
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
  t.datetime, t.T_out__C_, i.I_glob__W_m2_
from
  data_source_temperature t
  left join data_source_irradiance i on (t.datetime = i.datetime)
),

crop_data as (
%%CROP_DATA%%
),

crop_optimality as (
select
  crop_type,
  extract(month from datetime) as month,
  extract(day from datetime) as day,
  case
    when sum(case when hour_crop_optimality = 'optimal' then 1 else 0 end) >= 8 then 'optimal'
    when sum(case when hour_crop_optimality = 'optimal' or hour_crop_optimality = 'suboptimal' then 1 else 0 end) >= 6 then 'suboptimal'
    else 'unplantable'
  end day_crop_optimality
from (
  select
    d.datetime,
    c.crop_type,
    case
      when d.I_glob__W_m2_ <= 0 then 'night'
      when d.T_out__C_ >= c.optimal_min_temp and d.T_out__C_ <= c.optimal_max_temp then 'optimal'
      when d.T_out__C_ >= c.absolute_min_temp and d.T_out__C_ <= c.absolute_max_temp then 'suboptimal'
      else 'unplantable'
    end hour_crop_optimality
  from
    data_source d
    cross join crop_data c
  ) a
group by 1, 2, 3
),

crop_day_count as (
select
  crop_type,
  day_crop_optimality,
  count(*) num_days
from crop_optimality
  group by 1, 2
),

crop_cycle_count as (
select
  c.crop_type,
  floor(k.effective_days / (c.optimal_crop_cycle + %(cycle_exchange_period)s)) num_cycles,
  (c.optimal_crop_cycle + %(cycle_exchange_period)s) * floor(k.effective_days / (c.optimal_crop_cycle + %(cycle_exchange_period)s)) required_effective_days
from
  (select
    crop_type,
    sum(case
      when day_crop_optimality = 'optimal' then num_days
      when day_crop_optimality = 'suboptimal' then num_days * (1.0 / (1 + %(suboptimal_stretch_factor)s))
      else 0
    end) effective_days
  from crop_day_count
  group by 1
  ) k
  join crop_data c on (c.crop_type = k.crop_type)
),

crop_use_schedule as (
select
  co.crop_type, co.month, co.day,
  sum(case
    when co.day_crop_optimality = 'optimal' then 1
    when co.day_crop_optimality = 'suboptimal' then (1.0 / (1 + %(suboptimal_stretch_factor)s))
    else 0
  end) over (partition by co.crop_type order by co.month, co.day) <= cc.required_effective_days is_in_use
from
  crop_optimality co
  join crop_cycle_count cc on (co.crop_type = cc.crop_type)
),

complementary_crop_optimality as (
select
  extract(month from datetime) as month,
  extract(day from datetime) as day,
  case
    when sum(case when hour_crop_optimality = 'optimal' then 1 else 0 end) >= 8 then 'optimal'
    when sum(case when hour_crop_optimality = 'optimal' or hour_crop_optimality = 'suboptimal' then 1 else 0 end) >= 6 then 'suboptimal'
    else 'unplantable'
    end day_crop_optimality
from (
  select
    d.datetime,
    case
      when d.I_glob__W_m2_ <= 0 then 'night'
      when d.T_out__C_ >= %(complementary_crop_optimal_min_temp)s and d.T_out__C_ <= %(complementary_crop_optimal_max_temp)s then 'optimal'
      when d.T_out__C_ >= %(complementary_crop_absolute_min_temp)s and d.T_out__C_ <= %(complementary_crop_absolute_max_temp)s then 'suboptimal'
      else 'unplantable'
    end hour_crop_optimality
  from
    data_source d
) a
group by 1, 2
),

complementary_crop_optimality_with_availability as (
select
  crop_type,
  complementary_day_crop_optimality,
  count(*) num_days
from (
  select
    cus.crop_type,
    cus.month,
    cus.day,
    case
      when cus.is_in_use then 'main_crop'
      else cco.day_crop_optimality
    end complementary_day_crop_optimality
  from
    complementary_crop_optimality cco
    join crop_use_schedule cus on (cco.month = cus.month and cco.day = cus.day)
) a
group by 1, 2
),

complementary_crop_cycle_count as (
select
  crop_type,
  floor(k.effective_days / (%(complementary_crop_optimal_crop_cycle)s + %(cycle_exchange_period)s)) num_cycles
from (
  select
    crop_type,
    sum(case
      when complementary_day_crop_optimality = 'optimal' then num_days
      when complementary_day_crop_optimality = 'suboptimal' then num_days * (1.0 / (1 + %(suboptimal_stretch_factor)s))
      else 0
    end) effective_days
 from
   complementary_crop_optimality_with_availability
  group by 1
) k
)

select
  ccc.crop_type,
  ccc.num_cycles,
  comp_ccc.num_cycles num_complementary_cycles
from
  crop_cycle_count ccc
  join complementary_crop_cycle_count comp_ccc on (ccc.crop_type = comp_ccc.crop_type)
order by 1