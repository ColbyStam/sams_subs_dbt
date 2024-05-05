{{ config(
    materialized = 'table',
    schema = 'DW_SAMS_SUBS'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['store_id', 'store_address']) }} as store_key,
store_id,
store_address,
store_city,
store_state,
store_zip
FROM {{ source('sams_subs_landing', 'store') }}