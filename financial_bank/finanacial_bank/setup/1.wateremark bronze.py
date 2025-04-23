# Databricks notebook source
# MAGIC %sql
# MAGIC create database db_projects.dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table db_projects.dev.wateremark
# MAGIC (process_name string, watermark_time timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into db_projects.dev.wateremark
# MAGIC values
# MAGIC ('Ingest customers','1999-12-31T00:00:00.313+00:00'),
# MAGIC ('Ingest customer_drivers','1999-12-31T00:00:00.313+00:00'),
# MAGIC ('Ingest transactions','1999-12-31T00:00:00.313+00:00')
# MAGIC
# MAGIC
