# Databricks notebook source
# ==========================================================
# Notebook : Silver_Tariff
# Layer    : Silver
# Purpose  : Read Bronze Tariff Metrics Table
# ==========================================================

# Use Silver Catalog & Schema
spark.sql("USE CATALOG Silver_Catalog")
spark.sql("USE SCHEMA silver_sch")

# COMMAND ----------

# Read Bronze Tariff Metrics Table
tariff_df = spark.table(
    "Bronze_Catalog.bronze_sch.Bronze_Tariff_Metrics"
)

# COMMAND ----------

# Display Data
display(tariff_df)

# COMMAND ----------

# Verify Schema
tariff_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Data Transformation
# Source Table : Bronze_Tariff_Metrics
# Target Table : Silver_Tariff
# Purpose      : Convert columns to appropriate data types
# ==========================================================

# Import required functions
from pyspark.sql.functions import col

# ==========================================================
# Step 1 : Convert Numeric Columns from String to Double
# ==========================================================

tariff_df = (
    tariff_df
    .withColumn("unit_rate", col("unit_rate").cast("double"))
    .withColumn("peak_rate", col("peak_rate").cast("double"))
    .withColumn("offpeak_rate", col("offpeak_rate").cast("double"))
    .withColumn("fixed_charge", col("fixed_charge").cast("double"))
    .withColumn("tax_amount", col("tax_amount").cast("double"))
    .withColumn("subsidy_amount", col("subsidy_amount").cast("double"))
    .withColumn("monthly_bill", col("monthly_bill").cast("double"))
    .withColumn("billing_units", col("billing_units").cast("double"))
    .withColumn("late_fee", col("late_fee").cast("double"))
    .withColumn("adjustment_amount", col("adjustment_amount").cast("double"))
)

# ==========================================================
# Step 2 : Display Transformed Data
# ==========================================================

display(tariff_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

tariff_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Data Validation
# Source Table : Bronze_Tariff_Metrics
# Target Table : Silver_Tariff
# Purpose      : Validate data and remove invalid records
# ==========================================================

# Import required function
from pyspark.sql.functions import col

# ==========================================================
# Step 1 : Count Total Records Before Validation
# ==========================================================

print("Total Records Before Validation :", tariff_df.count())

# ==========================================================
# Step 2 : Remove Invalid Records
# Purpose : Remove records where mandatory columns are NULL
# ==========================================================

tariff_df = tariff_df.filter(
    col("household_id").isNotNull() &
    col("tariff_region").isNotNull() &
    col("tariff_city").isNotNull()
)

# ==========================================================
# Step 3 : Display Valid Records
# ==========================================================

display(tariff_df)

# ==========================================================
# Step 4 : Count Records After Validation
# ==========================================================

print("Total Valid Records :", tariff_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : Remove Duplicates
# Source Table : Bronze_Tariff_Metrics
# Target Table : Silver_Tariff
# Purpose      : Remove duplicate records from the dataset
# ==========================================================

# ==========================================================
# Step 1 : Count Records Before Removing Duplicates
# ==========================================================

print("Total Records Before Removing Duplicates :", tariff_df.count())

# ==========================================================
# Step 2 : Remove Duplicate Records
# Purpose : Keep only unique records
# ==========================================================

tariff_df = tariff_df.dropDuplicates()

# ==========================================================
# Step 3 : Display Records After Removing Duplicates
# ==========================================================

display(tariff_df)

# ==========================================================
# Step 4 : Count Records After Removing Duplicates
# ==========================================================

print("Total Records After Removing Duplicates :", tariff_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : Create Surrogate Key
# Source Table : Bronze_Tariff_Metrics
# Target Table : Silver_Tariff
# Purpose      : Generate a unique surrogate key for each record
# ==========================================================

# Import required function
from pyspark.sql.functions import monotonically_increasing_id

# ==========================================================
# Step 1 : Generate Surrogate Key
# ==========================================================

tariff_df = tariff_df.withColumn(
    "tariff_key",
    monotonically_increasing_id() + 1
)

# ==========================================================
# Step 2 : Display Data with Surrogate Key
# ==========================================================

display(tariff_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

tariff_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Implement SCD Type 2
# Source Table : Bronze_Tariff_Metrics
# Target Table : Silver_Tariff
# Purpose      : Track historical changes using SCD Type 2
# ==========================================================

# Import required functions
from pyspark.sql.functions import current_timestamp, lit

# ==========================================================
# Step 1 : Add SCD Type 2 Columns
# ==========================================================

tariff_df = (
    tariff_df
    .withColumn("effective_date", current_timestamp())
    .withColumn("end_date", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

# ==========================================================
# Step 2 : Display Data with SCD Type 2 Columns
# ==========================================================

display(tariff_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

tariff_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Load Data into Silver Delta Table
# Source Table : Bronze_Tariff_Metrics
# Target Table : Silver_Tariff
# Purpose      : Store transformed data in the Silver layer
# ==========================================================

try:
    (
        tariff_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("Silver_Catalog.silver_sch.Silver_Tariff")
    )

    print("Silver_Tariff Table Loaded Successfully")

except Exception as err:
    print("Failed to Load Silver_Tariff Table")
    print(str(err))

# COMMAND ----------

# ========================================================
# OPTIMIZE Silver Delta Table
# Purpose : Improve query performance
# ========================================================

spark.sql("""
    OPTIMIZE Silver_Catalog.silver_sch.Silver_Tariff
""")


# COMMAND ----------

# ========================================================
# ZORDER Silver Delta Table
# Purpose : Improve filtering performance
# ========================================================

spark.sql("""
    OPTIMIZE Silver_Catalog.silver_sch.Silver_Tariff
    ZORDER BY (household_id)
""")

# COMMAND ----------

# ========================================================
# VACUUM Silver Delta Table
# Purpose : Remove obsolete files and free storage space
# ========================================================

spark.sql("""
    VACUUM Silver_Catalog.silver_sch.Silver_Tariff RETAIN 168 HOURS
""")

# COMMAND ----------

# ========================================================
# Time Travel - View Table History
# Purpose : Display all available versions of the Delta table
# ========================================================

spark.sql("""
    DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Tariff
""").show(truncate=False)

# COMMAND ----------

# ========================================================
# Time Travel - Read Previous Version
# Purpose : Retrieve data from an earlier table version
# ========================================================

previous_version_df = spark.sql("""
    SELECT *
    FROM Silver_Catalog.silver_sch.Silver_Tariff
    VERSION AS OF 0
""")

display(previous_version_df)

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Tariff
""").select("version","timestamp").show(truncate=False)

# COMMAND ----------

# ========================================================
# Time Travel - Read Using Timestamp
# Purpose : Retrieve data using a valid timestamp
# ========================================================

previous_data = spark.sql("""
    SELECT *
    FROM Silver_Catalog.silver_sch.Silver_Tariff
    TIMESTAMP AS OF '2026-07-08 09:19:38'
""")

display(previous_data)