{{ config(
    materialized = 'table',
    schema = 'dw_sams_subs'
) }}

SELECT
    p.product_key,
    s.store_key,
    0 AS quantity_on_hand
FROM {{ ref('ss_dim_product') }} p
CROSS JOIN {{ ref('ss_dim_store') }} s