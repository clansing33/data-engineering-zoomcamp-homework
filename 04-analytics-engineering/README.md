### Question 1: Understanding dbt model resolution
What does this .sql model compile to?

```sql
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```

#### Answer
```sql
select * from myproject.my_nyc_tripdata.ext_green_taxi
```


### Question 2: dbt Variables & Dynamic Models

#### Answer
Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", "30") }}' DAY



### Question 3: dbt Data Lineage and Execution

#### Answer
```bash
dbt run --select +models/core/dim_taxi_trips.sql+ --target prod
```


### Question 4: dbt Macros and Jinja

#### Answer
- Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile
- ~~Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile~~
- When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET
- When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET
- When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET


### Quarter 5: Taxi Quarterly Revenue Growth

#### Answer
green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}


### Question 6: P97/P95/P90 Taxi Monthly Fare

#### Answer
green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}



### Question 7: Top #Nth longest P90 travel time Location for FHV

Ran the following for each dropoff zone to get the answer
```sql
select dropoff_zone,
       row_number() over (order by p90 desc) as p90_order
from dbt_clansing.fct_fhv_monthly_zone_traveltime_p90
where lower(pickup_zone) in ('yorkville east')
  and pickup_year = 2019
  and pickup_month = 11
order by p90_order
```


#### Answer
LaGuardia Airport, Chinatown, Garment District