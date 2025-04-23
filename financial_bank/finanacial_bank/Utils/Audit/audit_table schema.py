# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE or replace TABLE db_projects.dev.audit_table (
# MAGIC     job_id string,
# MAGIC     job_run_id string,
# MAGIC     taskRunId string,
# MAGIC     status string,
# MAGIC     rows_read int,
# MAGIC     operation string,
# MAGIC     rows_written int,
# MAGIC     target string,
# MAGIC     startTime timestamp,
# MAGIC     endTime timestamp,
# MAGIC     time_stamp timestamp
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from job_errors 

# COMMAND ----------


