# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls("abfss://raw@devdbde.dfs.core.windows.net")

# COMMAND ----------

name_var='Tushar'
print(f"My name is {name_var}")

# COMMAND ----------


# COMMAND ----------

Access_Key='sckhhW8bs9nO4vNOm+M1IYQxD+/dyfzbfitp06iiqY0QPIfTz+4LlvtMYmt0o0U5kveKJB5niXuG+AStk9ULsQ=='
storageaccount='devdbde'
spark.conf.set(
    f"fs.azure.account.key.{storageaccount}.dfs.core.windows.net",
    f"{Access_Key}")



# COMMAND ----------

dbutils.fs.ls("abfss://raw@devdbde.dfs.core.windows.net")
