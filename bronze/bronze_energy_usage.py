# Databricks notebook source
spark.sql("USE CATALOG Bronze_Catalog")
spark.sql("USE SCHEMA bronze_sch")

# COMMAND ----------

energy_df = spark.read.parquet(
    "abfss://projectcontainer@projectenergy.dfs.core.windows.net/parquetdata/energy_usage_stream_v2.parquet"
)

display(energy_df)

# COMMAND ----------

energy_df.printSchema()


# COMMAND ----------

# step 5.4 - Count Records
print(f"Total Records : {energy_df.count()}")

# COMMAND ----------

#Step 5 - Create Bronze Delta Table
energy_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Energy_Usage")

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM Bronze_Catalog.bronze_sch.Bronze_Energy_Usage
        LIMIT 10
    """)
)

# COMMAND ----------

#Step 6.1 – Add Audit Columns



from pyspark.sql.functions import current_timestamp, lit

energy_df = energy_df \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("energy_usage_stream_v2.parquet"))

#Now check:

display(energy_df)

#You should see two new columns:

#load_timestamp
#source_file

#Then overwrite the Bronze table again:

energy_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Energy_Usage", mode="overwrite", mergeSchema=True)

# COMMAND ----------

energy_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, max, to_timestamp

# Convert timestamp column from string to timestamp datatype
energy_df = energy_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# Check if Bronze table already exists
if spark.catalog.tableExists("Bronze_Catalog.bronze_sch.Bronze_Energy_Usage"):

    # Get the latest timestamp from Bronze table
    last_watermark = (
        spark.table("Bronze_Catalog.bronze_sch.Bronze_Energy_Usage")
             .agg(max("timestamp").alias("last_ts"))
             .collect()[0]["last_ts"]
    )

    print("Last Watermark :", last_watermark)

    # Load only new records
    if last_watermark is not None:
        energy_df = energy_df.filter(
            col("timestamp") > last_watermark
        )

else:
    print("First Load")

# COMMAND ----------

if last_watermark is not None:

    energy_df = energy_df.filter(
        col("timestamp") > last_watermark
    )

# COMMAND ----------

(
    energy_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Energy_Usage")
)

# COMMAND ----------

import traceback

try:

    (
        energy_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Energy_Usage")
    )

    print("Schema Evolution Implemented Successfully")

except Exception as err:

    print("Schema Evolution Failed")
    print("Error Message:", str(err))
    traceback.print_exc()

# COMMAND ----------

from pyspark.sql.functions import col, max, to_timestamp, lit

# Convert the dataframe's timestamp column from string to a proper timestamp type
energy_df = energy_df.withColumn(
    "timestamp", 
    to_timestamp(col("timestamp"))
)

# Check if the Bronze table already exists
if spark.catalog.tableExists("Bronze_Catalog.bronze_sch.Bronze_Energy_Usage"):
    # Get the latest timestamp from Bronze table
    last_watermark = (
        spark.table("Bronze_Catalog.bronze_sch.Bronze_Energy_Usage")
        .agg(max("timestamp").alias("last_ts"))
        .collect()[0]["last_ts"]
    )
    print("Last Watermark :", last_watermark)
    
    # Filter only new records using an explicit date-time format blueprint
    if last_watermark is not None:
        energy_df = energy_df.filter(
            col("timestamp") > to_timestamp(lit(last_watermark), "dd-MM-yyyy HH:mm")
        )
else:
    print("First Load")

# COMMAND ----------

import traceback

try:

    # Write Data into Bronze Table
    (
        energy_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Energy_Usage")
    )

    print("Data loaded successfully into Bronze_Energy_Usage")

except Exception as err:

    print("Error occurred while loading Bronze_Energy_Usage")
    print("Error Message:", str(err))

    traceback.print_exc()