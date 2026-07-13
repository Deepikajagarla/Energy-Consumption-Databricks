# Databricks notebook source
# ==========================================================
# Notebook : Silver_Device_Metrics
# Layer    : Silver
# Purpose  : Read Bronze Metrics Table
# ==========================================================

# Use Silver Catalog & Schema
spark.sql("USE CATALOG Silver_Catalog")
spark.sql("USE SCHEMA silver_sch")

# COMMAND ----------

# Read Bronze Device Metrics Table
device_df = spark.table(
    "Bronze_Catalog.bronze_sch.bronze_device_metrics"
)

# COMMAND ----------

spark.sql("""
SHOW TABLES IN Bronze_Catalog.bronze_sch
""").show(truncate=False)

# COMMAND ----------

# Display Data
display(device_df)

# COMMAND ----------


# Verify Schema
device_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Data Transformation
# Source Table : bronze_device_metrics
# Target Table : Silver_Device_Metrics
# Purpose      : Convert columns to appropriate data types
# ==========================================================

# Import required functions
from pyspark.sql.functions import col

# ==========================================================
# Step 1 : Convert Numeric Columns from String to Double
# ==========================================================

device_df = (
    device_df
    .withColumn("runtime_hours", col("runtime_hours").cast("double"))
    .withColumn("device_power_kw", col("device_power_kw").cast("double"))
    .withColumn("motor_speed_rpm", col("motor_speed_rpm").cast("double"))
    .withColumn("efficiency_ratio", col("efficiency_ratio").cast("double"))
    .withColumn("energy_draw_kwh", col("energy_draw_kwh").cast("double"))
    .withColumn("heat_output", col("heat_output").cast("double"))
    .withColumn("cooling_load", col("cooling_load").cast("double"))
    .withColumn("device_voltage", col("device_voltage").cast("double"))
    .withColumn("device_current", col("device_current").cast("double"))
)

# ==========================================================
# Step 2 : Display Transformed Data
# ==========================================================

display(device_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

device_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Data Validation
# Source Table : bronze_device_metrics
# Target Table : Silver_Device_Metrics
# Purpose      : Validate data and remove invalid records
# ==========================================================

# Import required function
from pyspark.sql.functions import col

# ==========================================================
# Step 1 : Count Total Records Before Validation
# ==========================================================

print("Total Records Before Validation :", device_df.count())

# ==========================================================
# Step 2 : Remove Invalid Records
# Purpose : Remove records where mandatory columns are NULL
# ==========================================================

device_df = device_df.filter(
    col("household_id").isNotNull() &
    col("device_category").isNotNull() &
    col("device_brand").isNotNull()
)

# ==========================================================
# Step 3 : Display Valid Records
# ==========================================================

display(device_df)

# ==========================================================
# Step 4 : Count Records After Validation
# ==========================================================

print("Total Valid Records :", device_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : Remove Duplicates
# Source Table : bronze_device_metrics
# Target Table : Silver_Device_Metrics
# Purpose      : Remove duplicate records from the dataset
# ==========================================================

# ==========================================================
# Step 1 : Count Records Before Removing Duplicates
# ==========================================================

print("Total Records Before Removing Duplicates :", device_df.count())

# ==========================================================
# Step 2 : Remove Duplicate Records
# ==========================================================

device_df = device_df.dropDuplicates()

# ==========================================================
# Step 3 : Display Records After Removing Duplicates
# ==========================================================

display(device_df)

# ==========================================================
# Step 4 : Count Records After Removing Duplicates
# ==========================================================

print("Total Records After Removing Duplicates :", device_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : Create Surrogate Key
# Source Table : bronze_device_metrics
# Target Table : Silver_Device_Metrics
# Purpose      : Generate a unique surrogate key for each record
# ==========================================================

# Import required function
from pyspark.sql.functions import monotonically_increasing_id

# ==========================================================
# Step 1 : Generate Surrogate Key
# ==========================================================

device_df = device_df.withColumn(
    "device_key",
    monotonically_increasing_id() + 1
)

# ==========================================================
# Step 2 : Display Data with Surrogate Key
# ==========================================================

display(device_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

device_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Implement SCD Type 2
# Source Table : bronze_device_metrics
# Target Table : Silver_Device_Metrics
# Purpose      : Track historical changes using SCD Type 2
# ==========================================================

# Import required functions
from pyspark.sql.functions import current_timestamp, lit

# ==========================================================
# Step 1 : Add SCD Type 2 Columns
# ==========================================================

device_df = (
    device_df
    .withColumn("effective_date", current_timestamp())
    .withColumn("end_date", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

# ==========================================================
# Step 2 : Display Data with SCD Type 2 Columns
# ==========================================================

display(device_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

device_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Load Data into Silver Delta Table
# Source Table : bronze_device_metrics
# Target Table : Silver_Device_Metrics
# Purpose      : Store transformed data in the Silver layer
# ==========================================================

try:
    (
        device_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("Silver_Catalog.silver_sch.Silver_Device_Metrics")
    )

    print("Silver_Device_Metrics Table Loaded Successfully")

except Exception as err:
    print("Failed to Load Silver_Device_Metrics Table")
    print(str(err))

# COMMAND ----------

# ========================================================
# OPTIMIZE Silver Delta Table
# Purpose : Improve query performance
# ========================================================

spark.sql("""
    OPTIMIZE Silver_Catalog.silver_sch.Silver_Device_Metrics
""")

# COMMAND ----------

# ========================================================
# ZORDER Silver Delta Table
# Purpose : Improve filtering performance
# ========================================================

spark.sql("""
    OPTIMIZE Silver_Catalog.silver_sch.Silver_Device_Metrics
    ZORDER BY (household_id)
""")

# COMMAND ----------

# ========================================================
# VACUUM Silver Delta Table
# Purpose : Remove obsolete files and free storage space
# ========================================================

spark.sql("""
    VACUUM Silver_Catalog.silver_sch.Silver_Device_Metrics RETAIN 168 HOURS
""")

# COMMAND ----------

# ========================================================
# Time Travel - View Table History
# Purpose : Display all available versions of the Delta table
# ========================================================

spark.sql("""
    DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Device_Metrics
""").show(truncate=False)

# COMMAND ----------

# ========================================================
# Time Travel - Read Previous Version
# Purpose : Retrieve data from an earlier table version
# ========================================================

previous_version_df = spark.sql("""
    SELECT *
    FROM Silver_Catalog.silver_sch.Silver_Device_Metrics
    VERSION AS OF 0
""")

display(previous_version_df)

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Device_Metrics
""").select("version", "timestamp").show(truncate=False)

# COMMAND ----------

# ========================================================
# Time Travel - Read Using Timestamp
# Purpose : Retrieve data using a valid timestamp
# ========================================================

previous_data = spark.sql("""
    SELECT *
    FROM Silver_Catalog.silver_sch.Silver_Device_Metrics
    TIMESTAMP AS OF '2026-07-08 09:40:36'
""")

display(previous_data)