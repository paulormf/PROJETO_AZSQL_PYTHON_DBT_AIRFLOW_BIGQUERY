with order_days as (
    select
        date(order_date, 'America/Sao_Paulo') as order_day
    from {{ ref('stg_orders') }}
    where order_date is not null
),

date_bounds as (
    select
        coalesce(
            min(order_day),
            current_date('America/Sao_Paulo')
        ) as first_day,
        greatest(
            coalesce(max(order_day), current_date('America/Sao_Paulo')),
            current_date('America/Sao_Paulo')
        ) as last_day
    from order_days
),

calendar as (
    select calendar_date
    from date_bounds
    cross join unnest(generate_date_array(first_day, last_day)) as calendar_date
)

select
    cast(format_date('%Y%m%d', calendar_date) as int64) as date_key,
    calendar_date as date_day,
    extract(year from calendar_date) as year,
    extract(quarter from calendar_date) as quarter,
    extract(month from calendar_date) as month,
    case extract(month from calendar_date)
        when 1 then 'janeiro'
        when 2 then 'fevereiro'
        when 3 then 'março'
        when 4 then 'abril'
        when 5 then 'maio'
        when 6 then 'junho'
        when 7 then 'julho'
        when 8 then 'agosto'
        when 9 then 'setembro'
        when 10 then 'outubro'
        when 11 then 'novembro'
        when 12 then 'dezembro'
    end as month_name,
    format_date('%Y-%m', calendar_date) as year_month,
    extract(day from calendar_date) as day_of_month,
    extract(dayofweek from calendar_date) as day_of_week,
    extract(dayofweek from calendar_date) in (1, 7) as is_weekend
from calendar
