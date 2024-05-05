{{ config(
    materialized = 'table',
    schema = 'DW_SAMS_SUBS'
) }}

SELECT
    c.customer_key,
    d.date_key,
    e.employee_key,
    p.product_key,
    s.store_key,
    ol.order_line_price as price,
    ol.order_line_qty AS quantity
FROM {{ source('sams_subs_landing', 'orderline') }} ol
INNER JOIN {{ source('sams_subs_landing', 'orders') }} ord ON ord.order_id = ol.order_id
INNER JOIN {{ ref('ss_dim_customer') }} c ON ord.customer_id = c.customer_id
INNER JOIN {{ ref('ss_dim_date') }} d ON d.date_day = ord.order_date
INNER JOIN {{ ref('ss_dim_employee') }} e ON ord.employee_id = e.employee_id
INNER JOIN {{ ref('ss_dim_product') }} p ON p.product_id = ol.product_id
INNER JOIN {{ ref('ss_dim_store') }} s ON s.store_id = ord.store_id