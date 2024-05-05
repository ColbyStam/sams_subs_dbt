{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['store_id', 'store_name']) }} as store_key,
store_id,
store_name,
street,
city,
state
FROM {{ source('oliver_landing', 'store') }}