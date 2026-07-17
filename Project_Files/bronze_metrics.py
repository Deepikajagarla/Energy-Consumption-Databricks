# Databricks notebook source
spark.sql("USE CATALOG Bronze_Catalog")
spark.sql("USE SCHEMA bronze_sch")

# COMMAND ----------

from pyspark.sql.functions import *
import traceback
device_df = (
    spark.read
         .option("mergeSchema","true")
         .parquet(
             "abfss://projectcontainer@projectenergy.dfs.core.windows.net/parquetdata//device_metrics_stream_v2.parquet"
         )
)

display(device_df)

# COMMAND ----------

device_df.printSchema()

# COMMAND ----------

print("Total Records :", device_df.count())

# COMMAND ----------

device_df = device_df \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("device_metrics_stream_v2.parquet"))

display(device_df)

# COMMAND ----------

from pyspark.sql.functions import max, col

if spark.catalog.tableExists(
    "Bronze_Catalog.bronze_sch.Bronze_Device_Metrics"
):

    last_watermark = (
        spark.table(
            "Bronze_Catalog.bronze_sch.Bronze_Device_Metrics"
        )
        .agg(max("load_timestamp").alias("last_ts"))
        .collect()[0]["last_ts"]
    )

    print("Last Watermark :", last_watermark)

    if last_watermark is not None:
        device_df = device_df.filter(
            col("load_timestamp") > last_watermark
        )

else:
    print("First Load")

# COMMAND ----------

try:

    (
        device_df.write
        .format("delta")
        .option("mergeSchema","true")
        .mode("append")
        .saveAsTable(
            "Bronze_Catalog.bronze_sch.Bronze_Device_Metrics"
        )
    )

    print("Load Successful")

except Exception as e:

    print("Load Failed")
    print(str(e))
    traceback.print_exc()

# COMMAND ----------

device_df.printSchema()

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM Bronze_Catalog.bronze_sch.Bronze_Device_Metrics
        LIMIT 10
    """)
)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS Total_Records
FROM Bronze_Catalog.bronze_sch.Bronze_Device_Metrics
""").show()

# COMMAND ----------

try:
    (
        device_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Device_Metrics")
    )
    # This must be indented inside the try block!
    print("Schema Evolution Implemented Successfully")

except Exception as err: # Changed 'e' to 'err' to fix the assignment error
    print("Schema Evolution Failed")
    print(str(err))
    traceback.print_exc()

# COMMAND ----------

from datetime import datetime
import traceback
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

load_start = datetime.now()

# Define the explicit audit schema cleanly
schema = StructType([
    StructField('table_name', StringType(), True),
    StructField('source_file', StringType(), True),
    StructField('load_start_time', TimestampType(), True),
    StructField('load_end_time', TimestampType(), True),
    StructField('records_loaded', IntegerType(), True),
    StructField('status', StringType(), True),
    StructField('error_message', StringType(), True)  # Changed to StringType to safely hold error text
])

try:
    # Count records before loading
    record_count = device_df.count()
    
    # Write to Bronze Table
    (
        device_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Device_Metrics")
    )
    
    load_end = datetime.now()
    
    # Create Success Audit Record
    audit_data = [
        ("Bronze_Device_Metrics", "device_metrics_stream_v2.parquet", load_start, load_end, int(record_count), "SUCCESS", None)
    ]
    audit_df = spark.createDataFrame(audit_data, schema)
    
    # Fixed syntax here: removed the space before the parenthesis
    audit_df.write.mode("append").saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Audit_Log")
    print("Load Successful")

except Exception as err:  # Changed 'e' to 'err'
    load_end = datetime.now()
    
    # Create Failed Audit Record
    audit_data = [
        ("Bronze_Device_Metrics", "device_metrics_stream_v2.parquet", load_start, load_end, 0, "FAILED", str(err))
    ]
    audit_df = spark.createDataFrame(audit_data, schema)
    
    audit_df.write.mode("append").saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Audit_Log")
    print("Load Failed")
    print(str(err))
    traceback.print_exc()

# COMMAND ----------

import traceback

try:

    # Write DataFrame to Bronze Table
    (
        device_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Device_Metrics")
    )

    print("Data loaded successfully into Bronze_Device_Metrics")

except Exception as err:

    print("Error occurred while loading data into Bronze_Device_Metrics")
    print("Error Message:", str(err))

    traceback.print_exc()

# COMMAND ----------

