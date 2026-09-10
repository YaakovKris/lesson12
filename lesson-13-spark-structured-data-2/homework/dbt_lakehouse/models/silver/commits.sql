with source as (
    select
        e.event_id,
        e.repo_name,
        e.actor_login as pushed_by,
        e.created_at as pushed_at,
        from_json(e.payload, {{ var('push_schema') }}) as payload
    from {{ ref('events') }} e
    where e.event_type = 'PushEvent'
),
exploded as (
    select
        s.event_id,
        s.repo_name,
        s.pushed_by,
        s.pushed_at,
        trim(regexp_replace(s.payload.ref, '^refs/heads/', '')) as branch,
        c.sha as commit_sha,
        c.message,
        c.author.name as author_name,
        c.author.email as author_email,
        c.`distinct` as is_distinct
    from source s
    lateral view explode_outer(s.payload.commits) c as c
    where c.sha is not null
),
ranked as (
    select
        *,
        row_number() over (partition by commit_sha order by pushed_at asc, event_id asc) as rn
    from exploded
)
select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    startswith(message, 'Merge ') as is_merge_commit,
    split(message, '\n')[0] as message_subject,
    length(message) as message_length
from ranked
where rn = 1
