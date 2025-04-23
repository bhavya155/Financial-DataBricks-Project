# Databricks notebook source
# MAGIC %sql
# MAGIC select * from audit_table order by starttime desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from job_errors
# MAGIC where jobId=993495637466956

# COMMAND ----------

Error:unsupported operand type(s) for +: 'int' and 'str'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.wateremark_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.feature_loan_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history dev.feature_loan_txn
