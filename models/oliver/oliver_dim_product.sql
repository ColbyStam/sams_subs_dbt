{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['product_id', 'product_name']) }} as product_key,
description,
product_id,
product_name,
unit_price
FROM {{ source('oliver_landing', 'product') }}