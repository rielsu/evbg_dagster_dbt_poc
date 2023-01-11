SELECT CAST (account_id AS NUMBER(18, 0)) AS account_id,
    CAST (organization_id AS NUMBER(18, 0)) AS organization_id,
    CAST (sub_id AS VARCHAR(120)) AS subscription_id,
    CAST (tt :active AS BOOLEAN) AS active,
    CAST (tt :uiPosition AS NUMBER(18, 0)) AS ui_position,
    CAST (tt :percentage AS NUMBER(18, 0)) AS percentage,
    ARRAY_TO_STRING(tt :emailList, ',') AS email_list,
    CAST (tt :triggered AS BOOLEAN) AS triggered,
    TO_DATE(parse_json(tt: lastAlertDate):"$date") AS last_alert_date
FROM (
        SELECT data :accountId AS account_id,
            -- Must explicitely pass null back if column not present
            case
                when data :organizationId is not null then data :organizationId
                else null
            end as organization_id,
            -- If an org has a valid subscription id, it takes percedence over the account's subscription id
            -- Note: The column name subscription_id cannot be used as it would be ambiguous thus it is called sub_id
            IFF(
                (
                    ORG_SUB.subscription_id IS NOT NULL
                    AND ORG_SUB.subscription_id <> ''
                ),
                ORG_SUB.subscription_id,
                ACCT_SUB.subscription_id
            ) AS sub_id,
            tt.value AS tt
        FROM {{ source('bpi','subscription_threshold_settings') }}
            LEFT JOIN {{ ref('stg_subscription_by_account') }} AS ACCT_SUB ON ACCT_SUB.account_id = CAST(DATA :accountId AS INTEGER)
            LEFT JOIN {{ ref('stg_subscription_by_organization') }} AS ORG_SUB ON ORG_SUB.organization_id = CAST(DATA :organizationId AS INTEGER),
            Table(
                Flatten(
                    {{ source('bpi','subscription_threshold_settings') }}.data :thresholdTriggers
                )
            ) AS tt
    )
-- Edge Case: It is possible a threshold was created for an account or org that the subscription
-- was later removed or some generated threshold settings that point to a non-existing account
WHERE subscription_id IS NOT NULL