select *
from {{ ref('fact_pull_request') }}
where churn != additions + deletions
