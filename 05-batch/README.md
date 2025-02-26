### Question 1: Install Spark and PySpark

```bash
pyspark
spark.version
```

#### Answer
3.3.2


### Question 2: Yellow October 2024

```bash
ls -lh *
```
#### Answer
25M


### Question 3: Count records

```python
import pyspark.sql.functions as F

(
    df0
    .withColumn("pickup_day", F.date_trunc('day', df0.tpep_pickup_datetime))
    .filter(F.col("pickup_day") == "2024-10-15 00:00:00")
    .count()
)
```

#### Answer
128,892


### Question 4: Longest trip

```python
from pyspark.sql.functions import expr

(
    df0
    .withColumn("hour_diff", expr("TIMESTAMPDIFF(HOUR, tpep_pickup_datetime, tpep_dropoff_datetime)"))
    .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "hour_diff")
    .sort("hour_diff", ascending=False)
    .show()
)
```

#### Answer
162


### Question 5: User Interface

#### Answer
4040


### Question 6: Least frequent pickup location zone

```python
(
    df0.join(df_tz, on=df0.PULocationID == df_tz.LocationID)
    .select("Zone", "tpep_pickup_datetime")
    .groupBy("Zone")
    .agg(F.count("*").alias("zone_count")) 
    .sort("zone_count")
).show()
```

#### Answer
Governor's Island/Ellis Island/Liberty Island