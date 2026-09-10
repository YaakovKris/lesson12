select *
from {{ ref('pull_requests') }}
where (merged_at is not null and merged_at < opened_at)
   or (closed_at is not null and closed_at < opened_at)
