# ============================================================
# EXPORT GOLD LAYER TABLES TO ADLS GEN2
# ============================================================

# ADLS Path
base_path = "abfss://projectcontainer@projectenergy.dfs.core.windows.net/golddata/"

CATALOG = "gold_catalog"
SCHEMA = "gold_sch"

gold_tables = {
    "dim_device": f"{CATALOG}.{SCHEMA}.dim_device",
    "dim_grid": f"{CATALOG}.{SCHEMA}.dim_grid",
    "dim_household": f"{CATALOG}.{SCHEMA}.dim_household",
    "dim_tariff": f"{CATALOG}.{SCHEMA}.dim_tariff",
    "dim_weather": f"{CATALOG}.{SCHEMA}.dim_weather",
    "fact_energy_usage": f"{CATALOG}.{SCHEMA}.fact_energy_usage",
    "daily_energy_summary": f"{CATALOG}.{SCHEMA}.daily_energy_summary",
    "monthly_energy_summary": f"{CATALOG}.{SCHEMA}.monthly_energy_summary",
    "yearly_energy_summary": f"{CATALOG}.{SCHEMA}.yearly_energy_summary",
    "billing_analytics": f"{CATALOG}.{SCHEMA}.billing_analytics",
    "device_monitoring": f"{CATALOG}.{SCHEMA}.device_monitoring",
    "executive_dashboard": f"{CATALOG}.{SCHEMA}.executive_dashboard",
    "weather_impact": f"{CATALOG}.{SCHEMA}.weather_impact"
}

print("="*80)
print("STARTING GOLD LAYER EXPORT")
print("="*80)

for folder_name, table_name in gold_tables.items():
    try:
        print(f"Exporting {table_name}")
        df = spark.table(table_name)
        (df.write.mode("overwrite").format("parquet").save(base_path + folder_name))
        print(f"SUCCESS : {folder_name}")
    except Exception as e:
        print(f"FAILED : {folder_name}")
        print(e)

print("="*80)
print("ALL GOLD TABLES EXPORTED")
print("="*80)
