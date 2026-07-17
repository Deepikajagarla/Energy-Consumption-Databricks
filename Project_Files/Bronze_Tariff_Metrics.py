# Databricks notebook source
spark.sql("USE CATALOG Bronze_Catalog")
spark.sql("USE SCHEMA bronze_sch")

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import traceback


# COMMAND ----------

tariff_df = (
    spark.read
         .option("mergeSchema", "true")
         .parquet(
             "abfss://projectcontainer@projectenergy.dfs.core.windows.net/parquetdata/tariff_metrics_stream_v2.parquet"
         )
)

# COMMAND ----------

display(tariff_df)

# COMMAND ----------

tariff_df.printSchema()

# COMMAND ----------

print("Total Records :", tariff_df.count())

# COMMAND ----------

import traceback

try:

    (
        tariff_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Tariff_Metrics")
    )

    print("Schema Evolution Implemented Successfully")

except Exception as err:

    print("Schema Evolution Failed")
    print("Error Message:", str(err))
    traceback.print_exc()

# COMMAND ----------

from datetime import datetime
import traceback
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Record the start time of the load process
load_start = datetime.now()

# 1. Define a strict blueprint schema matching your SQL table
# This explicitly enforces the 'INT' type for records_loaded
audit_schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("load_start_time", TimestampType(), True),
    StructField("load_end_time", TimestampType(), True),
    StructField("records_loaded", IntegerType(), True),  # Enforces matching SQL INT type
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True)
])

try:
    # Count the incoming records
    record_count = tariff_df.count()
    
    # 2. Write main data to the Bronze Tariff Metrics table
    (
        tariff_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Tariff_Metrics")
    )
    
    load_end = datetime.now()
    
    # 3. Build and write the SUCCESS Audit Log
    audit_data = [
        (
            "Bronze_Tariff_Metrics", 
            "tariff_metrics_stream_v2.parquet", 
            load_start, 
            load_end, 
            int(record_count), 
            "SUCCESS", 
            None
        )
    ]
    audit_df = spark.createDataFrame(audit_data, schema=audit_schema)
    audit_df.write.format("delta").mode("append").saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Audit_Log")
    
    print("Load Successful")

except Exception as err:
    load_end = datetime.now()
    
    # 4. Build and write the FAILED Audit Log if anything breaks
    audit_data = [
        (
            "Bronze_Tariff_Metrics", 
            "tariff_metrics_stream_v2.parquet", 
            load_start, 
            load_end, 
            0, 
            "FAILED", 
            str(err)
        )
    ]
    audit_df = spark.createDataFrame(audit_data, schema=audit_schema)
    audit_df.write.format("delta").mode("append").saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Audit_Log")
    
    print("Load Failed")
    print(str(err))
    traceback.print_exc()

# COMMAND ----------

import traceback

try:

    (
        tariff_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Tariff_Metrics")
    )

    print("Data loaded successfully into Bronze_Tariff_Metrics")

except Exception as err:

    print("Error occurred while loading Bronze_Tariff_Metrics")
    print("Error Message:", str(err))

    traceback.print_exc()

# COMMAND ----------

bronze_catalog.bronze_sch.bronze_weather