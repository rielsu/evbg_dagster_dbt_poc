--
-- This query will catch changes to active subscription periods even
-- when subscription starts further than a year back in time.
--
WITH total_usage as (
    SELECT usage.sub_id as sub_id,
      -- This cannot include subscription start/end date as it is possible that usage
      -- occurred within a current subscription that spans further than a year back
      -- and the starting and/or ending subscription period was altered later thus
      -- UEM will not adjust it and even if within a year, the yearly reaggregation
      -- only occurs 4-times a month
      --usage.start_date as start_date,
      --usage.end_date as end_date,
      sum(usage.credits_used) as credits_used,
      (sub.purchased_usage + sub.yearly_usage_allowance) as allowance,
      threshold_trigger.account_id as account_id,
      threshold_trigger.organization_id as organization_id,
      threshold_trigger.ui_position as ui_position,
      threshold_trigger.percentage as threshold_percentage,
      threshold_trigger.email_list as email_list,
      -- Added so that this can be reused to see all subscriptions that are over thresholds even if already triggered
      threshold_trigger.triggered
    FROM
      {{ ref('stg_credits_used_by_subscription')}} as usage,
      {{ ref('stg_subscription_threshold_triggers')}} as threshold_trigger,
      {{ ref('active_subscriptions')}} as sub
    WHERE usage.sub_id = threshold_trigger.subscription_id
      AND sub.subscription_id = threshold_trigger.subscription_id
      AND threshold_trigger.active = true
      -- NOTE: TO MAKE THIS REUSABLE FOR SEEING ALL THRESHOLDS REACHED THE FOLLOWING AND STATEMENT WOULD BE APPLIED IN THE DAILY PROCESS TO EXCLUDE THOSE THAT HAVE ALREADY BEEN TRIGGERED
      -- AND threshold_trigger.triggered = false
      -- It is possible that subscription start date that was over a year old was changed and as it stands
      -- currently, UEM only reprocesses usage going back a year, which means we need to look for usage that
      -- had an origional subscription start date that was greater than or equal to the current subscription
      -- start date and less than or equal to the current subscription end date as that too could have been
      -- modified from UEM data generated over a year ago.
      AND usage.start_date >= sub.start_date
      AND usage.end_date <= sub.end_date

      AND usage.billable = true
      AND usage.credits_used > 0
    GROUP BY
      usage.sub_id,
      threshold_trigger.account_id,
      threshold_trigger.organization_id,
      threshold_trigger.ui_position,
      threshold_trigger.triggered,
      allowance,
      threshold_percentage,
      email_list
    ORDER BY
      usage.sub_id,
      threshold_trigger.account_id,
      threshold_trigger.organization_id
    )
SELECT
  sub_id,
  account_id,
  organization_id,
  ui_position,
  credits_used,
  allowance,
  IFF(allowance = 0, 100, ROUND(credits_used / allowance * 100)) as percentage_used,
  threshold_percentage,
  email_list
FROM
  total_usage
WHERE
  percentage_used >= threshold_percentage
ORDER BY
  sub_id,
  threshold_percentage
