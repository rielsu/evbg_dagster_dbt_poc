WITH usage_data AS (
        SELECT DISTINCT
            -- TODO: Using DISTINCT that includes notification_id because there were duplicate notification entries
            -- at least in dev3. However, it might be better to instead use a window frame to select the duplicate
            -- with the latest reportcreateddate
            CAST(DATA :notificationId AS INTEGER) AS notification_id,
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
            CAST (DATA :subscriptionPeriodStartDate AS DATE) AS start_date,
            CAST (DATA :subscriptionPeriodEndDate AS DATE) AS end_date,
            --
            -- A flag that indicates whether usage should be billed:
            --   if subscription at the time of usage had excludeLifeSafety = true
            --     billable = usage not lifeSafety
            --   else
            --     billable = true
            --
            CAST (DATA :billable AS BOOLEAN) AS billable,
            CAST(LEFT(DATA :pathType, 32) AS VARCHAR(32)) AS path_type,
            CAST(DATA :points AS INTEGER) AS points,
            PARTITION_YEAR AS YEAR,
            PARTITION_MONTH AS MONTH
        FROM {{ source('bpi','usage_event_monthly') }}
            LEFT JOIN {{ ref('stg_subscription_by_account') }} AS ACCT_SUB ON ACCT_SUB.account_id = CAST(DATA :accountId AS INTEGER)
            LEFT JOIN {{ ref('stg_subscription_by_organization') }} AS ORG_SUB ON ORG_SUB.organization_id = CAST(DATA :organizationId AS INTEGER)
        WHERE DATA :subscriptionPeriodStartDate IS NOT NULL
            AND DATA :subscriptionPeriodStartDate <> ''
            AND DATA :subscriptionPeriodEndDate IS NOT NULL
            AND DATA :subscriptionPeriodEndDate <> ''
            AND DATA :points IS NOT NULL
            AND DATA :points <> ''
    )
SELECT sub_id,
    start_date,
    end_date,
    billable,
    path_type,
    YEAR,
    MONTH,
    SUM(points) AS credits_used
FROM usage_data
GROUP BY sub_id,
    start_date,
    end_date,
    billable,
    path_type,
    YEAR,
    MONTH