# Module 3: Data Warehouse Homework

### Creating the external table

Created the external table using the following query:

```sql
CREATE OR REPLACE EXTERNAL TABLE `data-warehouse-485615.nyc_taxi_2024.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_taxi_2024/yellow_tripdata_2024-*.parquet']
);
```
### Creating the regular table

Created the regular table in BigQuery using the following query:

```sql
CREATE OR REPLACE TABLE `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_non_partitioned`
AS (
SELECT * FROM `data-warehouse-485615.nyc_taxi_2024.external_yellow_tripdata`
);
```

# Quiz Questions

Q1. What is count of records for the 2024 Yellow Taxi Data?

Query to count the number of records for the 2024 Yellow Taxi Data:
```sql
SELECT COUNT(*)
FROM `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_non_partitioned`;
```

A: 20332093

Q2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

Query to count the distinct number of PULocationIDs from the external table
```sql
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `data-warehouse-485615.nyc_taxi_2024.external_yellow_tripdata`;
```

Query to count the distinct number of PULocationIDs from the materialized table
```sql
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_non_partitioned`;
```

A: 0 MB for the External Table and 155.12 MB for the Materialized Table

Q3. Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

Query to retrive the PULocationUD from the materialized table:
```sql
SELECT PULocationID
FROM `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_non_partitioned`;
```

Query to retrieve the PULocationID and DOLocationID from the materialized table:
```sql
SELECT PULocationID, DOLocationID
FROM `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_non_partitioned`;
```
A: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

Q4. How many records have a fare_amount of 0?

Query to count the number of records having fare_amount as 0:
```sql
SELECT COUNT(*)
FROM `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_non_partitioned`
WHERE fare_amount=0;
```

A: 8333

Q5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy) 

A: Partition by tpep_dropoff_datetime and Cluster on VendorID

Query to create the table using the optimized strategy:
```sql
CREATE OR REPLACE TABLE `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
SELECT * FROM `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_non_partitioned`
);
```

Q6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Query to retrieve distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 using the materialized table:
```sql
SELECT DISTINCT(VendorID)
FROM `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN "2024-03-01" AND "2024-03-15";
```

Query to retrieve distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 using the partitioned table:
```sql
SELECT DISTINCT(VendorID)
FROM `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_optimized`
WHERE DATE(tpep_dropoff_datetime) BETWEEN "2024-03-01" AND "2024-03-15";
```

A: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

Q7. Where is the data stored in the External Table you created?

A: GCP Bucket

Q8. It is best practice in Big Query to always cluster your data:

A: False

Q9. Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

Query to count the number of records for the 2024 Yellow Taxi Data:
```sql
SELECT COUNT(*)
FROM `data-warehouse-485615.nyc_taxi_2024.yellow_tripdata_non_partitioned`;
```
A: The estimated usage shows 0 bytes because BigQuery already knows exactly how many rows were added during the table creation process and stores the total count in its metadata.