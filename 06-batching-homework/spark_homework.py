"""
Homework analysis using PySpark with Pandas fallback for Java compatibility
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, max, min, count, floor, ceil, round
from pyspark.sql.functions import (unix_timestamp, lit)
from pyspark.sql.window import Window

# Q1 - Initialize Spark Session
try:
    spark = SparkSession.builder \
        .appName("YellowTaxiAnalysis") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.viewFs.impl.disable.cache", "true") \
        .getOrCreate()
    
    print("Spark version:", spark.version)
    
    # Load data
    df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
    
    # Q2 - Repartition into 4 files and save
    df.repartition(4).write.mode("overwrite").parquet("yellow_4parts")
    print("Repartitioned into 4 files")
    
    # Q3 - Count trips on Nov 15
    df = df.withColumn("tpep_pickup_date", to_date(col("tpep_pickup_datetime")))
    trips_nov15 = df.filter(col("tpep_pickup_date") == lit("2025-11-15")).count()
    print(f"Trips on Nov 15: {trips_nov15}")
    
    # Q4 - Longest trip in hours
    df = df.withColumn("trip_hours", 
        (unix_timestamp(col("tpep_dropoff_datetime")) - 
         unix_timestamp(col("tpep_pickup_datetime"))) / 3600)
    longest_trip_hours = df.agg(max("trip_hours")).collect()[0][0]
    print(f"Longest trip hours: {longest_trip_hours}")
    
    # Q6 - Least frequent pickup zone
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
    
    print(f"Least frequent zone: {least_frequent_zone} (count: {least_frequent_count})")
    
except Exception as e:
    print(f"Spark execution failed due to: {type(e).__name__}")
    print("Falling back to Pandas...")
    
    import pandas as pd
    import os
    
    # Load data with Pandas
    df = pd.read_parquet("yellow_tripdata_2025-11.parquet")
    
    # Q2 - Repartition (N/A for Pandas, but would save as 4 separate parquet files)
    os.makedirs("yellow_4parts", exist_ok=True)
    total_rows = len(df)
    rows_per_partition = total_rows // 4
    partitions = []
    for i in range(4):
        start_idx = i * rows_per_partition
        end_idx = start_idx + rows_per_partition if i < 3 else total_rows
        partition_df = df.iloc[start_idx:end_idx]
        partition_df.to_parquet(f"yellow_4parts/part_{i}.parquet")
        partitions.append(len(partition_df))
    
    print(f"Repartitioned into 4 files: {partitions}")
    
    # Q3 - Count trips on Nov 15
    df['tpep_pickup_date'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.date
    trips_nov15 = len(df[df['tpep_pickup_date'] == pd.to_datetime('2025-11-15').date()])
    print(f"Trips on Nov 15: {trips_nov15}")
    
    # Q4 - Longest trip in hours
    df['trip_hours'] = (pd.to_datetime(df['tpep_dropoff_datetime']) - pd.to_datetime(df['tpep_pickup_datetime'])).dt.total_seconds() / 3600
    longest_trip_hours = df['trip_hours'].max()
    print(f"Longest trip hours: {longest_trip_hours}")
    
    # Q6 - Least frequent pickup zone
    zones = pd.read_csv("taxi_zone_lookup.csv")
    pickup_counts = df['PULocationID'].value_counts()
    least_frequent_location = pickup_counts.idxmin()
    least_frequent_count = pickup_counts.min()
    
    # Map to zone name
    zone_info = zones[zones['LocationID'] == least_frequent_location]
    least_frequent_zone = zone_info['Zone'].values[0] if len(zone_info) > 0 else f"Location {least_frequent_location}"
    
    print(f"Least frequent zone: {least_frequent_zone} (count: {least_frequent_count})")
