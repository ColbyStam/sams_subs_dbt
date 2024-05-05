{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['agentid', 'phone']) }} as agent_key,
agentid,
firstname,
lastname,
email,
phone
FROM {{ source('insurance_landing', 'agents') }}