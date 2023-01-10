{{
    config(
        tags=['hourly'],
        materialized='incremental',
        on_schema_change='fail',
        unique_key='ALERT_ID'
    )
}}

with source_data AS (
  SELECT event.*, ROW_NUMBER() OVER (PARTITION BY ALERT_ID ORDER BY DWINSERTEDDATE DESC) AS rn
  FROM {{ ref('alerts_all_dagster') }} AS event
{% if is_incremental() %}
    where DWINSERTEDDATE > (select max(DWINSERTEDDATE) from {{ this }})
{% endif %}
)

select
  *
from source_data
where rn = 1
