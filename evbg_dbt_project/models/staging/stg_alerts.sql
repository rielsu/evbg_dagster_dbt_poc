select
    *
from {{ source('analytics', 'alerts') }}