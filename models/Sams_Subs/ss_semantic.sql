{{ config(
    materialized='table',
    schema='dw_sams_subs'
) }}

SELECT
    d.date_day AS order_date,
    c.customer_first_name AS cust_fname,
    e.employee_first_name AS emp_fname,
    p.product_name,
    s.store_address,
    o.price AS unit_price,
    o.quantity,
    (o.price * o.quantity) AS total_sale_amount
FROM {{ ref('ss_fact_order') }} o
INNER JOIN {{ ref('ss_dim_customer') }} c ON o.customer_key = c.customer_key
INNER JOIN {{ ref('ss_dim_date') }} d ON o.date_key = d.date_key
INNER JOIN {{ ref('ss_dim_employee') }} e ON o.employee_key = e.employee_key
INNER JOIN {{ ref('ss_dim_product') }} p ON o.product_key = p.product_key
INNER JOIN {{ ref('ss_dim_store') }} s ON o.store_key = s.store_key