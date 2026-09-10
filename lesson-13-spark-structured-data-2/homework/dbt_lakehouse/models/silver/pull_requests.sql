with parsed as (
    select
        e.repo_name,
        e.event_id,
        e.created_at as event_at,
        e.payload,
        from_json(e.payload, {{ var('pr_schema') }}) as payload_json
    from {{ ref('events') }} e
    where e.event_type = 'PullRequestEvent'
),
ranked as (
    select
        *,
        row_number() over (
            partition by repo_name, payload_json.number
            order by event_at desc, event_id desc
        ) as rn
    from parsed
),
latest as (
    select
        repo_name,
        payload_json.number as pr_number,
        payload_json.pull_request.title as title,
        payload_json.pull_request.user.login as author_login,
        payload_json.pull_request.state as state,
        payload_json.pull_request.merged as is_merged,
        payload_json.pull_request.draft as is_draft,
        to_timestamp(payload_json.pull_request.created_at) as opened_at,
        to_timestamp(payload_json.pull_request.closed_at) as closed_at,
        to_timestamp(payload_json.pull_request.merged_at) as merged_at,
        payload_json.pull_request.additions as additions,
        payload_json.pull_request.deletions as deletions,
        payload_json.pull_request.changed_files as changed_files,
        payload_json.pull_request.commits as commits_count,
        payload_json.pull_request.comments as comments,
        payload_json.pull_request.review_comments as review_comments,
        payload_json.pull_request.author_association as author_association,
        transform(payload_json.pull_request.labels, x -> x.name) as label_names,
        payload_json.action as last_action,
        event_at as last_event_at
    from ranked
    where rn = 1
)
select
    repo_name,
    pr_number,
    title,
    author_login,
    state,
    is_merged,
    is_draft,
    opened_at,
    closed_at,
    merged_at,
    additions,
    deletions,
    changed_files,
    commits_count,
    comments,
    review_comments,
    author_association,
    label_names,
    last_action,
    last_event_at,
    additions + deletions as churn,
    (unix_timestamp(coalesce(closed_at, last_event_at)) - unix_timestamp(opened_at)) / 3600.0 as hours_open
from latest
