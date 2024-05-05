{{ config(
    materialized = 'table',
    schema = 'DW_SAMS_SUBS'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['employee_id', 'employee_birthdate']) }} as employee_key,
employee_id,
employee_first_name,
employee_last_name,
employee_birthdate
FROM {{ source('sams_subs_landing', 'employee') }}