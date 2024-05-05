{{ config(
    materialized = 'table',
    schema = 'DW_SAMS_SUBS'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['product_id', 'product_name']) }} as product_key,
product_id,
product_type,
product_name,
product_cost,
product_calories,
sandwich_length
sandwich_bread_type
FROM {{ source('sams_subs_landing', 'product') }}