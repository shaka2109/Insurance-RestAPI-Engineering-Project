# Databricks notebook source
# DBTITLE 1,Import Utility Functions
# MAGIC %run ../misc/Utilities

# COMMAND ----------

# DBTITLE 1,Ingest Insurance Policies from REST API to Bronze Layer
error_msg = None
dataset_name = "insurance_policies"

try:
    df = fetch_rest_api_dataset(
        f"{dataset_name}", date_columns=["start_date", "end_date"]
    )
    writeDfToTable(df, "bronze", dataset_name)

except Exception as e:
    status = "FAILED"
    error_msg = str(e)
    log_pipeline_status("bronze", dataset_name, error_msg)
    print(f"✗ Failed to read {dataset_name}")
    print(f"Error: {error_msg}")
    raise Exception(error_msg)

# COMMAND ----------

# DBTITLE 1,Verify Bronze Table Data
# %sql
# SELECT *
# FROM insureallbi.bronze.insurance_policies
# LIMIT 10;
