select
    *
from {{ source('analytics', 'alerts_action_history') }}