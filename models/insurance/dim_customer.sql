{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['customerid', 'firstname']) }} as customer_key,
customerid,
firstname,
lastname,
dob,
address,
city,
state,
zipcode
FROM {{ source('insurance_landing', 'customers') }}