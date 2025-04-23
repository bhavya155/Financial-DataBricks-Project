# Databricks notebook source
# MAGIC %run "/Workspace/setup/0.commonvar"

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Audit/Audit_to_Table_func_succes"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Logging/2.Error Handling Code Table"
# MAGIC

# COMMAND ----------

def readSilver():
    return(spark.read.table("db_projects.dev.feature_loan_txn"))

def aggregate(loantranDf):
    from pyspark.sql.functions import sum,avg,min,max
    return(
        loantranDf.groupBy("date","payment_period",'currency_type','evaluation_chan','category').agg(
            # sum('loan_amount').alias('total_loan_amount'), 
            avg('loan_amount').alias('avg_loan_amount'),
            sum('current_debt').alias('sum_current_debt'),
            avg('interest_rate').alias('avg_interest_rate'),
            min('interest_rate').alias('min_interest_rate'),
            max('interest_rate').alias('max_interest_rate'),
            avg('health_score').alias('avg_score'),
            avg('monthly_salary').alias('avg_monthly_salary')
            )
    )
try:
    def process():
        print(f"\nStarting Silver Stream...", end='')
        loantranDf=readSilver()
        aggloantxnDf=aggregate(loantranDf)
        aggloantxnDf.write.mode("overwrite").saveAsTable("db_projects.dev.agg_loan_txn")
        print("Done")
        audit(loantranDf,'db_projects.dev.agg_loan_txn')
    process()
except Exception as e:
    error_message=f"Error:{e}"
    error_function(error_message)
    print(error_message)
            
        


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.agg_loan_txn
