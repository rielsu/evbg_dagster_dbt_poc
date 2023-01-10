{{
    config(
        tags=['hourly'],
        materialized='incremental',
        on_schema_change='fail'
    )
}}

SELECT
    payload:fullDocument._id AS ALERT_ID,
    payload:fullDocument.Organization AS ORGANIZATION_ID,
    payload:fullDocument.AcknowledgeAt::timestamp_ntz AS ACKNOWLEDGED_AT,
    payload:fullDocument.AffectedAssetCount AS AFFECTED_ASSET_COUNT,
    payload:fullDocument.ExpiresAt.DateTime::timestamp_ntz AS EXPIRES_AT,
    payload:fullDocument.ImpactArea AS IMPACT_AREA,
    payload:fullDocument.IsActive AS IS_ACTIVE,
    payload:fullDocument.Revision AS REVISION,
    payload:fullDocument.SnoozeUntil::timestamp_ntz AS SNOOZE_UNTIL,
    payload:fullDocument.RiskEvent.ID AS RISK_EVENT_ID,
    payload:fullDocument.RiskEvent.ExternalID AS RISK_EVENT_EXTERNAL_ID,
    payload:fullDocument.RiskEvent.Source AS RISK_EVENT_SOURCE,
    CASE
        WHEN payload:fullDocument.RiskEvent.Source IN (
            'NWS', -- NWS_PHENOMENON sender
            'Operator Entered Risk', -- USER_CREATED_RISK sender
            'Global Disaster Alerting Coordination System', -- legacy - GDACS_ITEM sender
            'USGS Earthquakes' -- legacy - USGS_FEATURE sender
        ) THEN RISK_EVENT_ID
        ELSE RISK_EVENT_EXTERNAL_ID
    END AS RISK_EVENT_JOIN_ID,
    payload:fullDocument.RiskEvent.ConfigurationID AS RISK_EVENT_CONFIGURATION_ID,

    DWINSERTEDDATE
  FROM {{ source('alerts_raw','ALERTS_LANDING') }}
  WHERE 1=1
{%- if is_incremental() %}
      -- We probably want to use something more deterministic than DWINSERTEDDATE since there
      -- may be instances where the DBT job is run while multiple rows are being inserted with
      -- the same identical insertiondate. The odds may be very low but it may be possible. The
      -- end result would be those rows added after the job will be skipped on subsequent runs.
      AND DWINSERTEDDATE > (select max(DWINSERTEDDATE) from {{ this }})
{%- else %}
      -- Only use alerts after 2022-10-01 for our POC
      AND DWINSERTEDDATE > '2022-10-01T00:00:00.000'
{%- endif %}
