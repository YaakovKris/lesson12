with parsed as (
    select
        e.repo_name,
        e.event_id,
        e.event_type,
        e.created_at as event_at,
        from_json(e.payload, {{ var('issue_schema') }}) as payload_json
    from {{ ref('events') }} e
    where e.event_type in ('IssuesEvent', 'IssueCommentEvent')
),
ranked as (
    select
        *,
        sum(case when event_type = 'IssueCommentEvent' then 1 else 0 end)
            over (partition by repo_name, payload_json.issue.number) as comment_events_seen,
        row_number() over (
            partition by repo_name, payload_json.issue.number
            order by event_at desc, event_id desc
        ) as rn
    from parsed
),
latest as (
    select
        repo_name,
        payload_json.issue.number as issue_number,
        payload_json.issue.title as title,
        payload_json.issue.user.login as author_login,
        payload_json.issue.state as state,
        to_timestamp(payload_json.issue.created_at) as opened_at,
        to_timestamp(payload_json.issue.closed_at) as closed_at,
        payload_json.issue.comments as comments,
        transform(payload_json.issue.labels, x -> x.name) as label_names,
        comment_events_seen,
        event_at as last_event_at
    from ranked
    where rn = 1
)
select
    repo_name,
    issue_number,
    title,
    author_login,
    state,
    opened_at,
    closed_at,
    comments,
    label_names,
    comment_events_seen,
    last_event_at,
    (unix_timestamp(closed_at) - unix_timestamp(opened_at)) / 3600.0 as hours_to_close
from latest
