# Databricks notebook source
# Cell 1 – Use Catalog & Schema
# ==========================================================
# Notebook : 03_Bronze_Weather
# Layer    : Bronze
# Purpose  : Load Weather Parquet into Bronze Delta Table
# ==========================================================

spark.sql("USE CATALOG Bronze_Catalog")
spark.sql("USE SCHEMA bronze_sch")


# COMMAND ----------

spark.sql("USE CATALOG Bronze_Catalog")
spark.sql("USE SCHEMA bronze_sch")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
weather_df = spark.read.parquet(
    "abfss://projectcontainer@projectenergy.dfs.core.windows.net/parquetdata//weather_stream_v2.parquet"
)

display(weather_df)


# COMMAND ----------

weather_df.printSchema()

# COMMAND ----------

print("Total Records :", weather_df.count())

# COMMAND ----------

weather_df = weather_df \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("weather_stream_v2.parquet"))

display(weather_df)

# COMMAND ----------

weather_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Weather")

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM Bronze_Catalog.bronze_sch.Bronze_Weather
        LIMIT 10
    """)
)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS Total_Records
FROM Bronze_Catalog.bronze_sch.Bronze_Weather
""").show()

# COMMAND ----------

from pyspark.sql.functions import col, max, to_timestamp

# Convert timestamp column from string to timestamp datatype
weather_df = weather_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# Check whether Bronze table already exists
if spark.catalog.tableExists(
    "Bronze_Catalog.bronze_sch.Bronze_Weather"
):

    # Get the latest processed timestamp
    last_watermark = (
        spark.table(
            "Bronze_Catalog.bronze_sch.Bronze_Weather"
        )
        .agg(max("timestamp").alias("last_ts"))
        .collect()[0]["last_ts"]
    )

    print("Last Watermark :", last_watermark)

    # Filter only new records
    if last_watermark is not None:
        weather_df = weather_df.filter(
            col("timestamp") > last_watermark
        )

else:
    print("First Load")

# COMMAND ----------

import traceback

try:

    (
        weather_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Weather")
    )

    print("Schema Evolution Implemented Successfully")

except Exception as err:

    print("Schema Evolution Failed")
    print("Error Message:", str(err))
    traceback.print_exc()

# COMMAND ----------

from pyspark.sql.functions import col, max, to_timestamp, expr

# Convert timestamp column from string to timestamp datatype
weather_df = weather_df.withColumn(
    "timestamp", 
    to_timestamp(col("timestamp"))
)

# Check whether Bronze table already exists
if spark.catalog.tableExists("Bronze_Catalog.bronze_sch.Bronze_Weather"):
    # Get the latest processed timestamp
    last_watermark = (
        spark.table("Bronze_Catalog.bronze_sch.Bronze_Weather")
        .agg(max("timestamp").alias("last_ts"))
        .collect()[0]["last_ts"]
    )
    print("Last Watermark :", last_watermark)
    
    # Filter only new records using a safe, explicit date parser format
    if last_watermark is not None:
        weather_df = weather_df.filter(
            col("timestamp") > to_timestamp(lit(last_watermark), "dd-MM-yyyy")
        )
else:
    print("First Load")

# COMMAND ----------

import traceback

try:

    # Write data into Bronze Table
    (
        weather_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("Bronze_Catalog.bronze_sch.Bronze_Weather")
    )

    print("Data loaded successfully into Bronze_Weather")

except Exception as err:

    print("Error occurred while loading Bronze_Weather")
    print("Error Message:", str(err))

    traceback.print_exc()