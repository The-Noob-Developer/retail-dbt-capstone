{{ config(
    materialized='table',
    schema='gold'
) }}

with ranked_employees as (

select
    *,
    row_number() over(
        partition by employee_id
        order by last_modified_date desc
    ) as rn

from {{ ref('stg_employee') }}

)

select

    row_number() over(order by employee_id) as employee_key,

    employee_id,

    full_name,

    standardized_role as role,

    work_location,

    tenure_years as tenure,

    email,

    phone,

    target_achievement_percentage as performance_metrics

from ranked_employees
where rn = 1