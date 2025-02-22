with cte_zones as (
    select *
    from {{ ref('dim_zones') }}
    where lower(borough) != 'unknown'
)

select
    fhv.*,
    extract(year from fhv.pickup_datetime) as pickup_year,
    extract(month from fhv.pickup_datetime) as pickup_month,
    zones_pu.borough as pickup_borough,
    zones_pu.zone as pickup_zone,
    zones_do.borough as dropoff_borough,
    zones_do.zone as dropoff_zone,
from {{ ref('stg_fhv') }} fhv
join cte_zones zones_pu
    on fhv.PUlocationID = zones_pu.locationid
join cte_zones zones_do
     on fhv.DOlocationID = zones_do.locationid