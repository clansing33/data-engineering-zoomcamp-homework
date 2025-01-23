
### Question 1. Understanding docker first run

```bash
docker run -it --entrypoint=bash python:3.12.8

pip -- version
```
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)

#### Answer
24.3.1

### Question 2. Understanding Docker networking and docker-compose

I am using vs code with a github codespace.  After running docker-compose up, I manually forwarded port 8080, using the Ports tab.

I was then able to access pgAdmin locally at http://localhost:8080

Inside pgAdmin I setup a connection to the database using "db" as the host name and port 5432.

#### Answer
db:5432


### Prepare Postgres

Python Ingest
```python
python ingest_data.py \
    --user=postgres \
    --password=postgres \
    --host=localhost \
    --port=5433 \
    --db=ny_taxi \
    --table_name=green_tripdata_10_2019
```

### Question 3. Trip Segmentation Count

Query
```sql
select 
    count(case when trip_distance <= 1 then 1 end) as up_to_1_mile,
    count(case when trip_distance > 1 and trip_distance <= 3 then 1 end) as between_1_and_3_miles,
    count(case when trip_distance > 3 and trip_distance <= 7 then 1 end) as between_3_and_7_miles,
    count(case when trip_distance > 7 and trip_distance <= 10 then 1 end) as between_7_and_10_miles,
    count(case when trip_distance > 10 then 1 end) as over_10_miles
from 
    public.green_tripdata_10_2019
where 
    lpep_dropoff_datetime  >= '2019-10-01' 
    and lpep_dropoff_datetime  < '2019-11-01';
```

#### Answer
104,802; 198,924; 109,603; 27,678; 35,189


### Question 4. Longest trip for each day
Query
```sql
select date(lpep_pickup_datetime) as pickup_date,
max(trip_distance) as max_trip_distance
from public.green_tripdata_10_2019
group by date(lpep_pickup_datetime)
order by max_trip_distance desc
```

#### Answer
2019-10-31


### Question 5. Three biggest pickup zones
Query
```sql
with cte as
(
select taxi_zones."Zone" as zone
from public.green_tripdata_10_2019 gtd
join public.taxi_zones
	on gtd."PULocationID" = taxi_zones."LocationID"
where date(lpep_pickup_datetime) = '10/18/2019'
group by taxi_zones."Zone"
having sum(total_amount) > 13000
)

select array_agg(zone) as zone from cte
```

#### Answer
East Harlem North, East Harlem South, Morningside Heights


### Question 6. Largest tip
Query
```sql
with cte_oct_ehn_pickups as
(
select "DOLocationID", gtd.tip_amount
from public.green_tripdata_10_2019 gtd
join public.taxi_zones
	on gtd."PULocationID" = taxi_zones."LocationID"
where extract(month from lpep_pickup_datetime) = 10
	and taxi_zones."Zone" = 'East Harlem North'
)

select taxi_zones."Zone", max(pu.tip_amount) as max_tip_amount
from cte_oct_ehn_pickups pu
join public.taxi_zones
	on pu."DOLocationID" = taxi_zones."LocationID"
group by taxi_zones."Zone"
order by max_tip_amount desc
limit 1
```

#### Answer
JFK Airport


### Question 7. Terraform Workflow

#### Answer
terraform init, terraform apply -auto-approve, terraform destroy