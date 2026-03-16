-- 2020-01-01 → 2030-12-31

{{ config(
    materialized='table',
    schema='gold'
) }}

with date_series as (

select
dateadd(day, seq4(), '2020-01-01') as full_date
from table(generator(rowcount => 3650))

)

select

to_number(to_char(full_date,'YYYYMMDD')) as date_key,

full_date,

year(full_date) as year,

quarter(full_date) as quarter,

month(full_date) as month,

week(full_date) as week,

dayofweek(full_date) as day_of_week,

case
when month(full_date) in (12,1,2) then 'Winter'
when month(full_date) in (3,4,5) then 'Spring'
when month(full_date) in (6,7,8) then 'Summer'
else 'Fall'
end as season,

case
when to_char(full_date,'MM-DD') in ('01-01','07-04','12-25')
then true
else false
end as holiday_flag

from date_series