
with 

base as (
SELECT 
user_id,
min(date_trunc(start, month)) over (partition by user_id) as cohort_month,  
sub_id,  
start, 
tier, 
price
FROM `general-319511.dwh.subscription_sample` -- Insert your table 
)

, base2 as (
select user_id, cohort_month, 
date_diff(date_trunc(start, month), cohort_month, month) as user_month, 
start, 
tier, price
from base
)

select 
cohort_month, 
user_month, 
count(distinct user_id) as users, 
sum(price) as revenue  
from base2 
group by 1,2 
order by 1,2 
