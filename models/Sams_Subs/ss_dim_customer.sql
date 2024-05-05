{{ config(
    materialized = 'table',
    schema = 'DW_SAMS_SUBS'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_first_name']) }} as customer_key,
customer_id,
customer_first_name,
customer_last_name,
customer_birthdate,
customer_phone
state
FROM {{ source('sams_subs_landing', 'customer') }}