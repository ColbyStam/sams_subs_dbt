{{ config(
    materialized='table',
    schema='dw_oliver_semantic'
) }}

SELECT
    d.date_day AS order_date,
    c.first_name AS cust_fname,
    e.first_name AS emp_fname,
    p.product_name,
    s.store_name,
    ol.unit_price,
    ol.quantity,
    (ol.unit_price * ol.quantity) AS total_sale_amount
FROM {{ ref('oliver_fact_orderline') }} ol
INNER JOIN {{ ref('oliver_dim_customer') }} c ON ol.customer_key = c.customer_key
INNER JOIN {{ ref('oliver_dim_date') }} d ON ol.date_key = d.date_key
INNER JOIN {{ ref('oliver_dim_employee') }} e ON ol.employee_key = e.employee_key
INNER JOIN {{ ref('oliver_dim_product') }} p ON ol.product_key = p.product_key
INNER JOIN {{ ref('oliver_dim_store') }} s ON ol.store_key = s.store_key
