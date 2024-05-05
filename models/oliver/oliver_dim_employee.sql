{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['employee_id', 'phone_number']) }} as employee_key,
email,
employee_id,
first_name,
hire_date,
last_name,
phone_number,
position
FROM {{ source('oliver_landing', 'employee') }}