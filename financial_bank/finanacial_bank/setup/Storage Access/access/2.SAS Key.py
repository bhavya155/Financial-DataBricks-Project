# Databricks notebook source
dbutils.fs.ls("abfss://raw@devmadeadlssa.dfs.core.windows.net")

# COMMAND ----------


# COMMAND ----------

SAS_TOKEN="sv=2022-11-02&ss=bfqt&srt=c&sp=rwdlacupyx&se=2024-11-14T17:56:23Z&st=2024-11-14T09:56:23Z&spr=https&sig=J6m2IwniSYMBGcG3cI7Noi7LbtKlXFv1%2BVFVSHsbe1A%3D"
storageaccount="devdbde"

spark.conf.set(f"fs.azure.account.auth.type.{storageaccount}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storageaccount}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storageaccount}.dfs.core.windows.net", f"{SAS_TOKEN}")








# COMMAND ----------

dbutils.fs.ls("abfss://raw@devdbde.dfs.core.windows.net")
