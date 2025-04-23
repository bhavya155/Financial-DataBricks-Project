# Databricks notebook source
# MAGIC %sql
# MAGIC create table db_projects.dev.wateremark_silver
# MAGIC (process_name string, watermark_time timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into db_projects.dev.wateremark_silver
# MAGIC values
# MAGIC ('transform customers','1999-12-31T00:00:00.313+00:00'),
# MAGIC ('transform customer_drivers','1999-12-31T00:00:00.313+00:00'),
# MAGIC ('transform transactions','1999-12-31T00:00:00.313+00:00')

# COMMAND ----------

# %sql
# update db_projects.dev.wateremark_silver
# set watermark_time="2025-01-03 10:26:31.064000"
# where process_name='transform customers'
