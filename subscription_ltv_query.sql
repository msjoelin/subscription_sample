

with 
--
base as (
	select 
	user_id, 
	sub_id, 
	start, 
	date_trunc(start, month) as start_month, 
	tier, 
	price 
	from general-319511.dwh.subscription_sample
--	limit 100 
)
--
,base2 as (
select *, 
min(start_month) over (partition by user_id)
as cohort_month
from base
)
,base3 as (
select *, 
date_diff(start, cohort_month, MONTH)
as user_month
from base2 
)
,base4 as (
select 
cohort_month, 
user_month, 
count(distinct user_id) as users, 
sum(price) as revenue
from base3
group by 1,2 
order by 1,2 
)
,base5 as (
select *, 
sum(revenue) over 
(partition by cohort_month order by user_month)
as revenue_cumulative
from base4
)
select * from base5 
order by cohort_month, user_month 
--limit 1000 
