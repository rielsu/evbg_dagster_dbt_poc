select 
    a.*,
    ah.actiontype,
    ah.actiontime,
    ah.previousversion
from {{ ref('stg_alerts') }} as a
join {{ ref('stg_alerts_action_history') }} as ah
on a.alertid = ah.alertid
