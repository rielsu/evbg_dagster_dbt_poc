SELECT CAST (subscription_id AS VARCHAR(120)) AS subscription_id,
    CAST (status AS VARCHAR(1)) AS status,
    CAST (ip :contractType AS VARCHAR(40)) AS contract_type,
    CAST (ip :startDate AS DATE) AS start_date,
    CAST (ip :endDate AS DATE) AS end_date,
    CAST (ip :excludeLifeSafety AS BOOLEAN) AS exclude_life_safety,
    CAST (ip :purchasedUsage AS NUMBER(18, 0)) AS purchased_usage,
    CAST (ip :useYearlyAllowance AS BOOLEAN) AS use_yearly_allowance,
    CAST (ip :yearlyUsageAllowance AS NUMBER(18, 0)) AS yearly_usage_allowance
FROM (
        SELECT data :id AS subscription_id,
            data :status AS status,
            ip.value AS ip
        FROM {{ source('bpi','subscription') }},
            Table(Flatten({{ source('bpi','subscription') }}.data :invoicingPeriods)) AS ip
    )