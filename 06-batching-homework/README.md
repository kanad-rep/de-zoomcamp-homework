# Module 6 - Batching Homework

Q1: Install Spark and PySpark

Initializing a local spark session:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YellowTaxiAnalysis") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.driver.memory", "2g") \
    .config("spark.hadoop.fs.viewFs.impl.disable.cache", "true") \
    .getOrCreate()

print("Spark version:", spark.version)
```

A: 4.1.1

Q2. What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)?

Partitioning the parquet file into smaller files:

```python
# Load data
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Repartition into 4 files and save
df.repartition(4).write.mode("overwrite").parquet("yellow_4parts")
print("Repartitioned into 4 files")
```

A: 25MB

Q3. How many taxi trips were there on the 15th of November?

Calculating the number of trips on 15th November:

```python
from pyspark.sql.functions import col, to_date, lit

df = df.withColumn("tpep_pickup_date", to_date(col("tpep_pickup_datetime")))
trips_nov15 = df.filter(col("tpep_pickup_date") == lit("2025-11-15")).count()
print(f"Trips on Nov 15: {trips_nov15}")
```

A: 162,604

Q4. What is the length of the longest trip in the dataset in hours?

Calculating the longest trip in hours

```python
from pyspark.sql.functions import col, unix_timestamp, max

df = df.withColumn("trip_hours", 
    (unix_timestamp(col("tpep_dropoff_datetime")) - 
        unix_timestamp(col("tpep_pickup_datetime"))) / 3600)
longest_trip_hours = df.agg(max("trip_hours")).collect()[0][0]
print(f"Longest trip hours: {longest_trip_hours}")
```

A: 90.6

Q5. Spark's User Interface which shows the application's dashboard runs on which local port?

A: 4040

Q6. Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

Finding the least frequest pickup location zone:

```python
from pyspark.sql.functions import col

zones_df = spark.read.csv("taxi_zone_lookup.csv", header=True)

pickup_counts = df.groupBy("PULocationID").count().orderBy("count")
least_frequent = pickup_counts.first()
least_frequent_location = least_frequent[0]
least_frequent_count = least_frequent[1]

# Map to zone name
zone_info = zones_df.filter(col("LocationID") == least_frequent_location)
zone_row = zone_info.collect()

if zone_row:
    least_frequent_zone = zone_row[0]["Zone"]
else:
    least_frequent_zone = f"Location {least_frequent_location}"

print(f"Least frequent zone: {least_frequent_zone}")
```

A: Governor's Island/Ellis Island/Liberty Island