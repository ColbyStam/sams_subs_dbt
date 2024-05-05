{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['customer_id', 'first_name']) }} as customer_key,
customer_id,
email,
first_name,
last_name,
phone_number,
state
FROM {{ source('oliver_landing', 'customer') }}