{{ config(
    materialized='table',
    schema='silver'
) }}

WITH json_extracted AS (

    SELECT
        {{ trim_whitespace("f.value:campaign_id::STRING") }}        AS campaign_id,
        {{ trim_whitespace("f.value:campaign_name::STRING") }}      AS campaign_name,

        {{ standardize_date("f.value:start_date") }}                AS start_date,
        {{ standardize_date("f.value:end_date") }}                  AS end_date,

        {{ trim_whitespace("f.value:budget::STRING") }}             AS budget,
        {{ trim_whitespace("f.value:target_audience::STRING") }}    AS target_audience,
        {{ trim_whitespace("f.value:campaign_type::STRING") }}      AS campaign_type,
        {{ trim_whitespace("f.value:channel::STRING") }}            AS channel,

        {{ trim_whitespace("f.value:total_cost::STRING") }}         AS total_cost,
        {{ trim_whitespace("f.value:total_revenue::STRING") }}      AS total_revenue,

        {{ trim_whitespace("f.value:roi_calculation::STRING") }}    AS roi_calculation,
        {{ trim_whitespace("f.value:description::STRING") }}        AS description,

        f.value:last_modified_date::DATE AS last_modified_date

    FROM RETAIL_DB.BRONZE.BR_CAMPAIGN t,
         LATERAL FLATTEN(input => t.value:campaigns_data) f

),

currency_clean AS (

    SELECT
        campaign_id,
        campaign_name,
        start_date,
        end_date,

        {{ clean_currency("budget") }}          AS budget_amount,
        {{ clean_currency("total_cost") }}      AS total_cost_amount,
        {{ clean_currency("total_revenue") }}   AS total_revenue_amount,

        target_audience,
        campaign_type,
        channel,
        description,
        last_modified_date

    FROM json_extracted

),

duration_calc AS (

    SELECT
        *,
        DATEDIFF(day, start_date, end_date) AS campaign_duration_days
    FROM currency_clean

),

audience_split AS (

    SELECT
        *,

        {{ trim_whitespace("SPLIT_PART(target_audience, ',', 1)") }} AS audience_group,
        {{ trim_whitespace("SPLIT_PART(target_audience, ',', 2)") }} AS age_range,
        {{ trim_whitespace("SPLIT_PART(target_audience, ',', 3)") }} AS location_type

    FROM duration_calc

),

roi_calc AS (

    SELECT
        *,
        (total_revenue_amount - total_cost_amount) /
        NULLIF(total_cost_amount, 0) AS roi
    FROM audience_split

),

deduplicated AS (

    SELECT DISTINCT *
    FROM roi_calc

)

SELECT *
FROM deduplicated