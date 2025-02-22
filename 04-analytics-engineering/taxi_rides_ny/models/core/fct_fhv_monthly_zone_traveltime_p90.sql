with cte_duration as (
    select *,
        time_diff(time(dropOff_datetime), time(pickup_datetime), second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
)

select pickup_year, pickup_month, pickup_zone, dropoff_zone,

percentile_cont(trip_duration, 0.90) over (
    partition by pickup_year, pickup_month, PUlocationID, DOlocationID
) as p90
from cte_duration
qualify row_number() over (partition by pickup_year, pickup_month, PUlocationID, DOlocationID order by trip_duration desc) = 1