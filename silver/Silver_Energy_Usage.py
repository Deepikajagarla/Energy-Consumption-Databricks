# Databricks notebook source
spark.sql("USE CATALOG Silver_Catalog")
spark.sql("USE SCHEMA silver_sch")

# COMMAND ----------

energy_df = spark.table(
    "Bronze_Catalog.bronze_sch.Bronze_Energy_Usage"
)

display(energy_df)

# COMMAND ----------

energy_df.printSchema()

# COMMAND ----------



# COMMAND ----------

display(energy_df.select(col("timestamp").cast("string")))

# COMMAND ----------

# ==========================================================
# Silver Layer : Data Transformation
# Source Table : Bronze_Energy_Usage
# Target Table : Silver_Energy_Usage
# Purpose      : Convert columns to appropriate data types
# ==========================================================

# Import required functions
from pyspark.sql.functions import col, to_timestamp

# Convert numeric columns from String to Double
energy_df = (
    energy_df
    .withColumn("voltage_reading", col("voltage_reading").cast("double"))
    .withColumn("current_reading", col("current_reading").cast("double"))
    .withColumn("active_power_kw", col("active_power_kw").cast("double"))
    .withColumn("reactive_power_kvar", col("reactive_power_kvar").cast("double"))
    .withColumn("energy_usage_kwh", col("energy_usage_kwh").cast("double"))
    .withColumn("frequency_hz", col("frequency_hz").cast("double"))
    .withColumn("load_factor", col("load_factor").cast("double"))
    .withColumn("peak_demand_kw", col("peak_demand_kw").cast("double"))
    .withColumn("offpeak_demand_kw", col("offpeak_demand_kw").cast("double"))
    .withColumn("daily_consumption_kwh", col("daily_consumption_kwh").cast("double"))
)

# Convert timestamp column from String to Timestamp
energy_df = energy_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm")
)

# Display transformed data
display(energy_df)

# Verify updated schema
energy_df.printSchema()

# COMMAND ----------

print("Total Records before  Removing Duplicates :", energy_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : Data Validation
# Source Table : Bronze_Energy_Usage
# Target Table : Silver_Energy_Usage
# Purpose      : Validate data before loading into Silver
# ==========================================================

from pyspark.sql.functions import col

# ==========================================================
# Step 1 : Remove records having NULL Household ID
# Household ID is the primary identifier and should not be NULL
# ==========================================================

energy_df = energy_df.filter(col("household_id").isNotNull())

# ==========================================================
# Step 2 : Remove records having NULL Timestamp
# Timestamp is mandatory for time-based analysis
# ==========================================================

energy_df = energy_df.filter(col("timestamp").isNotNull())

# ==========================================================
# Step 3 : Remove records having negative Energy Usage
# Energy usage cannot be less than zero
# ==========================================================

energy_df = energy_df.filter(col("energy_usage_kwh") >= 0)

# ==========================================================
# Step 4 : Display Validated Data
# ==========================================================

display(energy_df)

# ==========================================================
# Step 5 : Count Records After Validation
# ==========================================================

print("Total Valid Records :", energy_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : Create Surrogate Key
# Source Table : Bronze_Energy_Usage
# Target Table : Silver_Energy_Usage
# Purpose      : Generate a unique surrogate key for each record
# ==========================================================

# Import required function
from pyspark.sql.functions import monotonically_increasing_id

# ==========================================================
# Step 1 : Create Surrogate Key
# Purpose : Generate a unique ID for every record
# ==========================================================

energy_df = energy_df.withColumn(
    "energy_sk",
    monotonically_increasing_id()
)

# ==========================================================
# Step 2 : Display Data
# ==========================================================

display(energy_df)

# ==========================================================
# Step 3 : Verify Total Records
# ==========================================================

print("Total Records :", energy_df.count())

# COMMAND ----------

# ==========================================================
# Silver Layer : SCD Type 2
# Source Table : Bronze_Energy_Usage
# Target Table : Silver_Energy_Usage
# Purpose      : Add SCD Type 2 columns
# ==========================================================

from pyspark.sql.functions import current_timestamp, lit

# ==========================================================
# Step 1 : Add SCD Type 2 Columns
# ==========================================================

energy_df = (
    energy_df
    .withColumn("effective_start_date", current_timestamp())
    .withColumn("effective_end_date", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

# Display data
display(energy_df)

# Verify schema
energy_df.printSchema()

# COMMAND ----------

# ==========================================================
# Silver Layer : Load Data into Silver Delta Table
# Source Table : Bronze_Energy_Usage
# Target Table : Silver_Energy_Usage
# ==========================================================

(
    energy_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("Silver_Catalog.silver_sch.Silver_Energy_Usage")
)

print("Silver_Energy_Usage table created successfully.")

# COMMAND ----------



# Optimize Silver Delta Table to improve query performance
spark.sql("OPTIMIZE Silver_Catalog.silver_sch.Silver_Energy_Usage")

# COMMAND ----------

# ========================================================
# ZORDER Silver Delta Table
# Purpose : Improve filtering performance
# ========================================================

spark.sql("""
    OPTIMIZE Silver_Catalog.silver_sch.Silver_Energy_Usage
    ZORDER BY (household_id)
""")

# COMMAND ----------

# ========================================================
# VACUUM Silver Delta Table
# Purpose : Remove obsolete files from the Delta table
#           to free up storage space
# Retention : 168 Hours (7 Days)
# ========================================================

spark.sql("""
    VACUUM Silver_Catalog.silver_sch.Silver_Energy_Usage
    RETAIN 168 HOURS
""")

print("VACUUM completed successfully.")

# COMMAND ----------

# ========================================================
# Time Travel - View Table History
# Purpose : Display all available versions of the Delta table
# ========================================================

spark.sql("""
    DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Energy_Usage
""").show(truncate=False)

# COMMAND ----------

# ========================================================
# Time Travel - Read Previous Version
# Purpose : Retrieve data from an earlier table version
# ========================================================

previous_version_df = spark.sql("""
    SELECT *
    FROM Silver_Catalog.silver_sch.Silver_Energy_Usage
    VERSION AS OF 0
""")

display(previous_version_df)

# COMMAND ----------

# ========================================================
# Time Travel - Read Using Timestamp
# Purpose : Retrieve data using a valid timestamp
# ========================================================

previous_data = spark.sql("""
    SELECT *
    FROM Silver_Catalog.silver_sch.Silver_Energy_Usage
    TIMESTAMP AS OF '2026-07-07 11:10:32'
""")

display(previous_data)



# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY Silver_Catalog.silver_sch.Silver_Energy_Usage
""").select("version","timestamp").show(truncate=False)