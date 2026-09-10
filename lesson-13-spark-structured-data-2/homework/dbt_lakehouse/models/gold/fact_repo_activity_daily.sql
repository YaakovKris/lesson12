with commit_daily as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(pushed_at, 'yyyyMMdd') as int) as date_id,
        count(*) as commits,
        count(distinct author_email) as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('commits') }}
    group by md5(repo_name), cast(date_format(pushed_at, 'yyyyMMdd') as int)
),
pr_daily as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(opened_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        count(*) as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('pull_requests') }}
    group by md5(repo_name), cast(date_format(opened_at, 'yyyyMMdd') as int)
    union all
    select
        md5(repo_name) as repo_id,
        cast(date_format(merged_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        count(*) as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('pull_requests') }}
    where merged_at is not null
    group by md5(repo_name), cast(date_format(merged_at, 'yyyyMMdd') as int)
),
issue_daily as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(opened_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        count(*) as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('issues') }}
    group by md5(repo_name), cast(date_format(opened_at, 'yyyyMMdd') as int)
    union all
    select
        md5(repo_name) as repo_id,
        cast(date_format(closed_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        count(*) as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('issues') }}
    where closed_at is not null
    group by md5(repo_name), cast(date_format(closed_at, 'yyyyMMdd') as int)
),
watch_fork_daily as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(created_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        sum(case when event_type = 'WatchEvent' then 1 else 0 end) as stars,
        sum(case when event_type = 'ForkEvent' then 1 else 0 end) as forks
    from {{ ref('events') }}
    where event_type in ('WatchEvent', 'ForkEvent')
    group by md5(repo_name), cast(date_format(created_at, 'yyyyMMdd') as int)
),
combined as (
    select * from commit_daily
    union all
    select * from pr_daily
    union all
    select * from issue_daily
    union all
    select * from watch_fork_daily
),
rolled as (
    select
        repo_id,
        date_id,
        coalesce(sum(commits), 0) as commits,
        coalesce(sum(distinct_committers), 0) as distinct_committers,
        coalesce(sum(prs_opened), 0) as prs_opened,
        coalesce(sum(prs_merged), 0) as prs_merged,
        coalesce(sum(issues_opened), 0) as issues_opened,
        coalesce(sum(issues_closed), 0) as issues_closed,
        coalesce(sum(stars), 0) as stars,
        coalesce(sum(forks), 0) as forks
    from combined
    group by repo_id, date_id
)
select
    md5(concat_ws('|', repo_id, cast(date_id as string))) as activity_id,
    repo_id,
    date_id,
    commits,
    distinct_committers,
    prs_opened,
    prs_merged,
    issues_opened,
    issues_closed,
    stars,
    forks
from rolled
