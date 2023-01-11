SELECT subscription_id,
    contract_type,
    start_date,
    end_date,
    exclude_life_safety,
    purchased_usage,
    use_yearly_allowance,
    yearly_usage_allowance
FROM {{ ref('stg_subscriptions')}}
WHERE status = 'A'
    AND start_date IS NOT NULL
    -- Note: It will be assumed that the USER or SESSION will be set to UTC Timezone
    -- Example: alter session set timezone='UTC'
    AND TO_DATE(start_date) <= CURRENT_DATE()
    AND TO_DATE(end_date) >= CURRENT_DATE()
