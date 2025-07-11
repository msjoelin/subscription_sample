-- SQL Code for subscription metrics (Bigquery)

with 

dates as 
(
select date from  UNNEST(GENERATE_DATE_ARRAY('2023-01-01', '2023-12-31')) AS date
) 

, subscriptions as (select * from `your-table`)


-- ACTIVE SUBSCRIBERS 

, active_subs as
(
  select date, 
  count(distinct user_id) as active_subscribers 
  from dates d
  left join subscriptions s on d.date between s.start_date and s.end_date
  group by 1 order by 1 
)



-- NEW SUBSCRIBERS -- 
, new_subs as (
  select start_date as date, count(distinct user_id) as new_subscribers 
  from (
    SELECT USER_ID, MIN(START_DATE) AS START_DATE
    FROM subscriptions
    group by user_id
  ) 
  group by 1 
  order by 1 
) 

-- REACTIVATED SUBSCRIBERS --

,react_base as (
  SELECT user_id, start_date,end_date, 
  LAG(end_date) OVER (PARTITION BY user_id ORDER BY start_date) AS prev_end
  FROM subscriptions
)

, reactivations as (
select start_date as date, 
count(distinct user_id) as reactivated_subscribers 
from react_base
where date_diff(start_date, prev_end, day) >1
group by 1 
order by 1 
)


-- CHURN SUBSCRIBERS --

, churn_base as 
(
  SELECT user_id, start_date,end_date, 
  LEAD(start_date) OVER (PARTITION BY user_id ORDER BY start_date) AS next_start
  FROM subscriptions
)


, churns as (
select end_date+1 as date, count(distinct user_id) as churned_subscribers  
from churn_base 
where DATE_DIFF(next_start, end_date, DAY) > 1 OR next_start IS NULL
group by end_date
order by end_date
)





-- Final Join



select d.date, 
a.active_subscribers,
n.new_subscribers,
r.reactivated_subscribers, 
c.churned_subscribers, 
coalesce(n.new_subscribers, 0) + coalesce(r.reactivated_subscribers, 0) - coalesce(c.churned_subscribers, 0) as net_adds
from dates d 
left join active_subs a on d.date = a.date
left join new_subs n on d.date = n.date
left join reactivations r on d.date = r.date
left join churns c on d.date = c.date
order by d.date 

























