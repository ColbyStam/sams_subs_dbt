{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['policyid', 'policytype']) }} as policy_key,
policyid,
policytype
FROM {{ source('insurance_landing', 'policies') }}