### Question 1: 
What is count of records for the 2024 Yellow Taxi Data?

1. Manually downloaded files
2. Uploaded files to bucket manually created in gcp
```
gcloud storage cp *.parquet gs://de-03/
```

#### Create External Table
```sql
create or replace external table de_03.yellow_2024
    options (
    format ='parquet',
    uris = ['gs://de-03/*']
    );
```

#### Create BigQuery Table
```sql
create or replace table de_03.yellow_2024_bq
as
    select * from de_03.yellow_2024
```

#### Get record count
```sql
select count(1) from de_03.yellow_2024_bq
```

#### Answer
20,332,093


### Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

Queries
```sql
select count(distinct PULocationID)
from de_03.yellow_2024

select count(distinct PULocationID)
from de_03.yellow_2024_bq
```

#### Answer
0 MB for the External Table and 155.12 MB for the Materialized Table


### Question 3:
Write a query to retrieve the PULocationID form the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

Queries
```sql
select PULocationID
from de_03.yellow_2024_bq;

select PULocationID, DOLocationID
from de_03.yellow_2024_bq;
```

#### Answer
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


### Question 4:
How many records have a fare_amount of 0?

Query
```sql
select count(1) as count_fare_amount_0
from de_03.yellow_2024_bq
where fare_amount = 0
```

#### Answer
8,333


### Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_timedate and order the results by VendorID (Create a new table with this strategy)

Create Table (partitioned and clustered)
```sql
create or replace table de_03.yellow_2024_bg_partition_cluster
partition by date(tpep_dropoff_datetime)
cluster by VendorID
as

select * from de_03.yellow_2024_bq;
```

#### Answer
Partition by tpep_dropoff_datetime and Cluster on VendorID



### Question 6: 
Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

Queries
```sql
select distinct VendorID
from de_03.yellow_2024_bq
where date(tpep_dropoff_datetime) >= '2024-03-01'
    and date(tpep_dropoff_datetime) <= '2024-03-15';
-- 310.24 MB

select distinct VendorID
from de_03.yellow_2024_bg_partition_cluster
where date(tpep_dropoff_datetime) >= '2024-03-01'
    and date(tpep_dropoff_datetime) <= '2024-03-15';
-- 26.84 MB
```

#### Answer
310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

### Question 7:
Where is the data stored in the External Table you created?

#### Answer
GCP Bucket

### Question 8:
It is best practice in Big Query to always cluster your data:

#### Answer
False

##### Bonus
0 B  
BigQuery has this value in metadata, and does not need to read the data (for non-external tables)