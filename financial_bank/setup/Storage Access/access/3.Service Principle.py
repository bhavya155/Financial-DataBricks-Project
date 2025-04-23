# Databricks notebook source
client_id = "905c33a8-bac4-40a9-b7f7-db8c70b254f5"
tenant_id = "eb9a96c4-8083-422b-b5a4-c76b365aceb4"             
client_secret = "eIn8Q~quaTiHH_rZUoV_JJypg2ET.BgfVmBz7bKa"
account_storage="devadlsde"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{account_storage}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{account_storage}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_storage}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_storage}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{account_storage}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://raw@devdbde.dfs.core.windows.net")
