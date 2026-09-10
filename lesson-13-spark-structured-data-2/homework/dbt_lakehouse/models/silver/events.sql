{{ config(materialized='incremental', incremental_strategy='append') }}

with source as (
    select
        id as event_id,
        type as event_type,
        actor.login as actor_login,
        repo.name as repo_name,
        split(repo.name, '/')[0] as repo_owner,
        to_timestamp(created_at) as created_at,
        payload,
        _ingested_at,
        _source_file
    from {{ source('bronze', 'raw_events') }}
    where type in (
        'PushEvent',
        'PullRequestEvent',
        'IssuesEvent',
        'IssueCommentEvent',
        'WatchEvent',
        'ForkEvent'
    )
      and public is true
      and id is not null
      and repo.name is not null
      and created_at is not null
),
ranked as (
    select
        *,
        row_number() over (partition by event_id order by _ingested_at desc) as rn
    from source
)
select
    event_id,
    event_type,
    actor_login,
    repo_name,
    repo_owner,
    created_at,
    payload,
    _ingested_at,
    _source_file
from ranked
where rn = 1
{% if is_incremental() %}
  and _ingested_at > (
      select coalesce(max(_ingested_at), cast('1970-01-01 00:00:00' as timestamp))
      from {{ this }}
  )
{% endif %}
