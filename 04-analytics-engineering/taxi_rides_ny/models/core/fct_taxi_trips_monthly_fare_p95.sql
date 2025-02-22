select
    service_type,
    pickup_year, pickup_month,

    percentile_cont(fare_amount, 0.97) over (
        partition by service_type, pickup_year, pickup_month
    ) as p97,
    percentile_cont(fare_amount, 0.95) over (
        partition by service_type, pickup_year, pickup_month
    ) as p95,
    percentile_cont(fare_amount, 0.90) over (
        partition by service_type, pickup_year, pickup_month
    ) as p90

from {{ ref('fact_trips') }}
where fare_amount > 0
    and trip_distance > 0
    and lower(payment_type_description) in ('cash', 'credit card')
    and pickup_year in (2019, 2020)
qualify row_number() over (partition by service_type, pickup_year, pickup_month order by fare_amount desc) = 1