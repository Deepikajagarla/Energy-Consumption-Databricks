# Databricks notebook source
# ==========================================================
# Notebook : Silver_Weather
# Layer    : Silver
# Purpose  : Read Bronze Weather Table
# ==========================================================

# Use Silver Catalog & Schema
spark.sql("USE CATALOG Silver_Catalog")
spark.sql("USE SCHEMA silver_sch")

# COMMAND ----------

# Read Bronze Weather Table
weather_df = spark.table(
    "Bronze_Catalog.bronze_sch.Bronze_Weather"
)


# COMMAND ----------

# Display data
display(weather_df)

# COMMAND ----------

# Verify schema
weather_df.printSchema()

# COMMAND ----------

weather_df = spark.table(
    "Bronze_Catalog.bronze_sch.Bronze_Weather"
)

# COMMAND ----------

# ==========================================================
# Silver Layer : Data Transformation
# Source Table : Bronze_Weather
# Target Table : Silver_Weather
# Purpose      : Convert columns to appropriate data types
# ==========================================================

# Import required functions
from pyspark.sql.functions import col, to_timestamp

# ==========================================================
# Step 1 : Read Bronze Weather Table Again
# ==========================================================

weather_df = spark.table("Bronze_Catalog.bronze_sch.Bronze_Weather")

# ==========================================================
# Step 2 : Convert Numeric Columns from String to Double
# ==========================================================

weather_df = (
    weather_df
    .withColumn("temperature_celsius", col("temperature_celsius").cast("double"))
    .withColumn("humidity_percent", col("humidity_percent").cast("double"))
    .withColumn("wind_speed_kmh", col("wind_speed_kmh").cast("double"))
    .withColumn("rainfall_mm", col("rainfall_mm").cast("double"))
    .withColumn("pressure_hpa", col("pressure_hpa").cast("double"))
    .withColumn("solar_radiation", col("solar_radiation").cast("double"))
    .withColumn("dew_point", col("dew_point").cast("double"))
    .withColumn("uv_index", col("uv_index").cast("double"))
    .withColumn("visibility_km", col("visibility_km").cast("double"))
    .withColumn("cloud_cover_percent", col("cloud_cover_percent").cast("double"))
)

# ==========================================================
# Step 3 : Convert Timestamp Column
# Format : dd-MM-yyyy
# ==========================================================

weather_df = weather_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "dd-MM-yyyy")
)

# ==========================================================
# Step 4 : Display Transformed Data
# ==========================================================

display(weather_df)

# ==========================================================
# Step 5 : Verify Updated Schema
# ==========================================================

weather_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Data Validation
# Source Table : Bronze_Weather
# Target Table : Silver_Weather
# Purpose      : Validate data and remove invalid records
# ==========================================================

from pyspark.sql.functions import col

# ==========================================================
# Step 1 : Count Total Records Before Validation
# ==========================================================

print("Total Records Before Validation :", weather_df.count())

# ==========================================================
# Step 2 : Remove Invalid Records
# Purpose : Remove records where mandatory columns are NULL
# ==========================================================

weather_df = weather_df.filter(
    col("household_id").isNotNull() &
    col("weather_region").isNotNull() &
    col("weather_city").isNotNull() &
    col("timestamp").isNotNull()
)

# ==========================================================
# Step 3 : Display Valid Records
# ==========================================================

display(weather_df)

# ==========================================================
# Step 4 : Count Records After Validation
# ==========================================================

print("Total Valid Records :", weather_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : Remove Duplicates
# Source Table : Bronze_Weather
# Target Table : Silver_Weather
# Purpose      : Remove duplicate records from the dataset
# ==========================================================

# ==========================================================
# Step 1 : Count Records Before Removing Duplicates
# ==========================================================

print("Total Records Before Removing Duplicates :", weather_df.count())

# ==========================================================
# Step 2 : Remove Duplicate Records
# Purpose : Keep only unique records
# ==========================================================

weather_df = weather_df.dropDuplicates()

# ==========================================================
# Step 3 : Display Records After Removing Duplicates
# ==========================================================

display(weather_df)

# ==========================================================
# Step 4 : Count Records After Removing Duplicates
# ==========================================================

print("Total Records After Removing Duplicates :", weather_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : Create Surrogate Key
# Source Table : Bronze_Weather
# Target Table : Silver_Weather
# Purpose      : Generate a unique surrogate key for each record
# ==========================================================

# Import required function
from pyspark.sql.functions import monotonically_increasing_id

# ==========================================================
# Step 1 : Generate Surrogate Key
# ==========================================================

weather_df = weather_df.withColumn(
    "weather_key",
    monotonically_increasing_id() + 1
)

# ==========================================================
# Step 2 : Display Data with Surrogate Key
# ==========================================================

display(weather_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

weather_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Implement SCD Type 2
# Source Table : Bronze_Weather
# Target Table : Silver_Weather
# Purpose      : Track historical changes using SCD Type 2
# ==========================================================

# Import required functions
from pyspark.sql.functions import current_timestamp, lit

# ==========================================================
# Step 1 : Add SCD Type 2 Columns
# ==========================================================

weather_df = (
    weather_df
    .withColumn("effective_date", current_timestamp())
    .withColumn("end_date", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

# ==========================================================
# Step 2 : Display Data with SCD Type 2 Columns
# ==========================================================

display(weather_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

weather_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Load Data into Silver Delta Table
# Source Table : Bronze_Weather
# Target Table : Silver_Weather
# Purpose      : Store transformed data in the Silver layer
# ==========================================================

try:
    (
        weather_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("Silver_Catalog.silver_sch.Silver_Weather")
    )

    print("Silver_Weather Table Loaded Successfully")

except Exception as err:
    print("Failed to Load Silver_Weather Table")
    print(str(err))

# COMMAND ----------

# ========================================================
# OPTIMIZE Silver Delta Table
# Purpose : Improve query performance
# ========================================================

spark.sql("""
    OPTIMIZE Silver_Catalog.silver_sch.Silver_Weather
""")

# COMMAND ----------

# ========================================================
# ZORDER Silver Delta Table
# Purpose : Improve filtering performance
# ========================================================

spark.sql("""
    OPTIMIZE Silver_Catalog.silver_sch.Silver_Weather
    ZORDER BY (household_id)
""")

# COMMAND ----------

# ========================================================
# VACUUM Silver Delta Table
# Purpose : Remove obsolete files and free storage space
# ========================================================

spark.sql("""
    VACUUM Silver_Catalog.silver_sch.Silver_Weather RETAIN 168 HOURS
""")

# COMMAND ----------

# ========================================================
# Time Travel - View Table History
# Purpose : Display all available versions of the Delta table
# ========================================================

spark.sql("""
    DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Weather
""").show(truncate=False)

# COMMAND ----------

# ========================================================
# Time Travel - Read Using Timestamp
# Purpose : Retrieve data using a valid timestamp
# ========================================================

previous_data = spark.sql("""
    SELECT *
    FROM Silver_Catalog.silver_sch.Silver_Weather
    TIMESTAMP AS OF '2026-07-07 12:30:16'
""")

display(previous_data)

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Weather
""").select("version", "timestamp").show(truncate=False)

# COMMAND ----------

previous_version_df = spark.sql("""
    SELECT *
    FROM Silver_Catalog.silver_sch.Silver_Weather
    VERSION AS OF 0
""")

display(previous_version_df)