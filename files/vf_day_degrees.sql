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
)

select
  sum(cdd) cdd, sum(hdd) hdd
from (
  select
    extract(month from datetime), extract(day from datetime), greatest(avg(T_out__C_) - %(cdd_lower_bound_C)s, 0) cdd, greatest(%(hdd_higher_bound_C)s - avg(T_out__C_), 0) hdd
  from
    data_source
  group by 1, 2
) a