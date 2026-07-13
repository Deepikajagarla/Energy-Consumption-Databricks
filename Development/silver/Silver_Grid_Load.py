# Databricks notebook source
# ==========================================================
# Notebook : Silver_Grid_Load
# Layer    : Silver
# Purpose  : Read Bronze Grid Load Table
# ==========================================================

# Use Silver Catalog & Schema
spark.sql("USE CATALOG Silver_Catalog")
spark.sql("USE SCHEMA silver_sch")


# COMMAND ----------


# Read Bronze Grid Load Table
grid_df = spark.table(
    "Bronze_Catalog.bronze_sch.Bronze_Grid_Load"
)

# COMMAND ----------

# Display data
display(grid_df)

# COMMAND ----------

# Verify schema
grid_df.printSchema()

# COMMAND ----------

# Target Table : Silver_Grid_Load
# Purpose      : Convert columns to appropriate data types
# ==========================================================

# Import required functions
from pyspark.sql.functions import col

# ==========================================================
# Step 1 : Convert Numeric Columns from String to Double
# ==========================================================

grid_df = (
    grid_df
    .withColumn("grid_voltage", col("grid_voltage").cast("double"))
    .withColumn("grid_current", col("grid_current").cast("double"))
    .withColumn("grid_load_kw", col("grid_load_kw").cast("double"))
    .withColumn("transformer_load", col("transformer_load").cast("double"))
    .withColumn("line_loss_percent", col("line_loss_percent").cast("double"))
    .withColumn("load_variation", col("load_variation").cast("double"))
    .withColumn("frequency_variation", col("frequency_variation").cast("double"))
    .withColumn("grid_capacity_kw", col("grid_capacity_kw").cast("double"))
    .withColumn("demand_forecast_kw", col("demand_forecast_kw").cast("double"))
    .withColumn("reserve_margin", col("reserve_margin").cast("double"))
)

# ==========================================================
# Step 2 : Display Transformed Data
# ==========================================================

display(grid_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

grid_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Data Validation
# Source Table : Bronze_Grid_Load
# Target Table : Silver_Grid_Load
# Purpose      : Validate data and remove invalid records
# ==========================================================

# Import required functions
from pyspark.sql.functions import col

# ==========================================================
# Step 1 : Count Total Records Before Validation
# ==========================================================

print("Total Records Before Validation :", grid_df.count())

# ==========================================================
# Step 2 : Remove Invalid Records
# Purpose : Remove records where mandatory columns are NULL
# ==========================================================

grid_df = grid_df.filter(
    col("household_id").isNotNull() &
    col("grid_region").isNotNull() &
    col("substation_name").isNotNull()
)

# ==========================================================
# Step 3 : Display Valid Records
# ==========================================================

display(grid_df)

# ==========================================================
# Step 4 : Count Records After Validation
# ==========================================================

print("Total Valid Records :", grid_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : Remove Duplicates
# Source Table : Bronze_Grid_Load
# Target Table : Silver_Grid_Load
# Purpose      : Remove duplicate records from the dataset
# ==========================================================

# ==========================================================
# Step 1 : Count Records Before Removing Duplicates
# ==========================================================

print("Total Records Before Removing Duplicates :", grid_df.count())

# ==========================================================
# Step 2 : Remove Duplicate Records
# Purpose : Keep only unique records
# ==========================================================

grid_df = grid_df.dropDuplicates()

# ==========================================================
# Step 3 : Display Records After Removing Duplicates
# ==========================================================

display(grid_df)

# ==========================================================
# Step 4 : Count Records After Removing Duplicates
# ==========================================================

print("Total Records After Removing Duplicates :", grid_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : Create Surrogate Key
# Source Table : Bronze_Grid_Load
# Target Table : Silver_Grid_Load
# Purpose      : Generate a unique surrogate key for each record
# ==========================================================

# Import required function
from pyspark.sql.functions import monotonically_increasing_id

# ==========================================================
# Step 1 : Generate Surrogate Key
# ==========================================================

grid_df = grid_df.withColumn(
    "grid_key",
    monotonically_increasing_id() + 1
)

# ==========================================================
# Step 2 : Display Data with Surrogate Key
# ==========================================================

display(grid_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

grid_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Implement SCD Type 2
# Source Table : Bronze_Grid_Load
# Target Table : Silver_Grid_Load
# Purpose      : Track historical changes using SCD Type 2
# ==========================================================

# Import required functions
from pyspark.sql.functions import current_timestamp, lit

# ==========================================================
# Step 1 : Add SCD Type 2 Columns
# ==========================================================

grid_df = (
    grid_df
    .withColumn("effective_date", current_timestamp())
    .withColumn("end_date", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

# ==========================================================
# Step 2 : Display Data with SCD Type 2 Columns
# ==========================================================

display(grid_df)

# ==========================================================
# Step 3 : Verify Updated Schema
# ==========================================================

grid_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Load Data into Silver Delta Table
# Source Table : Bronze_Grid_Load
# Target Table : Silver_Grid_Load
# Purpose      : Store transformed data in the Silver layer
# ==========================================================

try:
    (
        grid_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("Silver_Catalog.silver_sch.Silver_Grid_Load")
    )

    print("Silver_Grid_Load Table Loaded Successfully")

except Exception as err:
    print("Failed to Load Silver_Grid_Load Table")
    print(str(err))

# COMMAND ----------

# ========================================================
# OPTIMIZE Silver Delta Table
# Purpose : Improve query performance
# ========================================================

spark.sql("""
    OPTIMIZE Silver_Catalog.silver_sch.Silver_Grid_Load
""")

# COMMAND ----------

# ========================================================
# ZORDER Silver Delta Table
# Purpose : Improve filtering performance
# ========================================================

spark.sql("""
    OPTIMIZE Silver_Catalog.silver_sch.Silver_Grid_Load
    ZORDER BY (household_id)
""")

# COMMAND ----------

# ========================================================
# VACUUM Silver Delta Table
# Purpose : Remove obsolete files and free storage space
# ========================================================

spark.sql("""
    VACUUM Silver_Catalog.silver_sch.Silver_Grid_Load RETAIN 168 HOURS
""")

# COMMAND ----------

# ========================================================
# Time Travel - View Table History
# Purpose : Display all available versions of the Delta table
# ========================================================

spark.sql("""
    DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Grid_Load
""").show(truncate=False)

# COMMAND ----------

# ========================================================
# Time Travel - Read Previous Version
# Purpose : Retrieve data from an earlier table version
# ========================================================

previous_version_df = spark.sql("""
    SELECT *
    FROM Silver_Catalog.silver_sch.Silver_Grid_Load
    VERSION AS OF 0
""")

display(previous_version_df)

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Grid_Load
""").select("version","timestamp").show(truncate=False)

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Grid_Load
""").select("version", "timestamp").show(truncate=False)

# COMMAND ----------

# ========================================================
# Time Travel - Read Using Timestamp
# Purpose : Retrieve data using a valid timestamp
# ========================================================

previous_data = spark.sql("""
    SELECT *
    FROM Silver_Catalog.silver_sch.Silver_Grid_Load
    TIMESTAMP AS OF '2026-07-08 09:02:21'
""")

display(previous_data)