
with 

base as ( 
  select user_id, 
  date_trunc(min(start) over (partition by user_id), month) as cohort_month, 
  first_value(tier) over (partition by user_id order by start asc) as first_tier, 
  date_trunc(start, month) as active_month, 
  tier
  FROM `general-319511.dwh.subscription_raw_data`
) 
-- add a month counter / user month column
, base2 as (
  select *, 
  date_diff(active_month, cohort_month, month) as user_month
  from base 
)

, active_users as  ( 
select cohort_month, first_tier, user_month, 
count(distinct user_id) as active_users
from base2
group by 1,2, 3
)

, cohort_size as (
  select cohort_month, first_tier, count(distinct user_id) as cohort_size
from base2
group by 1,2 
)

select a.*, b.cohort_size 
from active_users a 
left join cohort_size b using(cohort_month, first_tier)
order by cohort_month, first_tier, user_month
