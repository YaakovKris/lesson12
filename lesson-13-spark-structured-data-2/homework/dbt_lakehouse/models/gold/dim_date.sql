with date_bounds as (
    select
        min(date_day) as min_date,
        max(date_day) as max_date
    from (
        select cast(date_format(pushed_at, 'yyyy-MM-dd') as date) as date_day from {{ ref('commits') }}
        union all
        select cast(date_format(opened_at, 'yyyy-MM-dd') as date) as date_day from {{ ref('pull_requests') }}
        union all
        select cast(date_format(merged_at, 'yyyy-MM-dd') as date) as date_day from {{ ref('pull_requests') }} where merged_at is not null
        union all
        select cast(date_format(opened_at, 'yyyy-MM-dd') as date) as date_day from {{ ref('issues') }}
        union all
        select cast(date_format(closed_at, 'yyyy-MM-dd') as date) as date_day from {{ ref('issues') }} where closed_at is not null
    ) d
),
calendar as (
    select explode(sequence(to_date(min_date), to_date(max_date), interval 1 day)) as date_day
    from date_bounds
)
select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_id,
    cast(date_day as date) as date_day,
    dayofweek(date_day) as day_of_week,
    dayofweek(date_day) in (1, 7) as is_weekend,
    weekofyear(date_day) as iso_week,
    year(date_day) as year
from calendar
