select *
from (
    select sum(commits) as total_commits
    from {{ ref('fact_repo_activity_daily') }}
) a
cross join (
    select count(*) as total_fact_commits
    from {{ ref('fact_commit') }}
) b
where a.total_commits != b.total_fact_commits
