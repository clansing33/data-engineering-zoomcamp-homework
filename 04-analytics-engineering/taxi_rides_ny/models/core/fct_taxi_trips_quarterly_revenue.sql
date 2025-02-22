with cte_2019 as (
    select service_type, year_quarter, pickup_year, pickup_quarter, sum(total_amount) as revenue
    from {{ ref('fact_trips') }}
    where pickup_year = 2019
    group by service_type, year_quarter, pickup_year, pickup_quarter
),

cte_2020 as (
    select service_type, year_quarter, pickup_year, pickup_quarter, sum(total_amount) as revenue
    from {{ ref('fact_trips') }}
    where pickup_year = 2020
    group by service_type, year_quarter, pickup_year, pickup_quarter
)

select
    g19.service_type, g19.pickup_quarter,

    g19.revenue as revenue_19, g20.revenue as revenue_20,
    (g20.revenue - g19.revenue) / g19.revenue as growth

from cte_2019 g19
join cte_2020 g20
    on g19.service_type = g20.service_type
    and g19.pickup_quarter = g20.pickup_quarter