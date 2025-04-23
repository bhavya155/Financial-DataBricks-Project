# Databricks notebook source
# MAGIC %run "/Workspace/setup/0.commonvar"

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Audit/Audit_to_Table_func_succes"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Logging/2.Error Handling Code Table"
# MAGIC

# COMMAND ----------

last_processed_time_cusdri=spark.read.table("db_projects.dev.wateremark_silver").filter("process_name=='transform customer_drivers'").select("watermark_time").collect()[0][0]

# COMMAND ----------

print(last_processed_time_cusdri)

# COMMAND ----------

last_loaded_time_cusdri=spark.read.table("db_projects.dev.wateremark").filter("process_name='Ingest customer_drivers'").select("watermark_time").collect()[0][0]

# COMMAND ----------

print(last_loaded_time_cusdri)

# COMMAND ----------

last_processed_time_loans=spark.read.table("db_projects.dev.wateremark_silver").filter("process_name=='transform transactions'").select("watermark_time").collect()[0][0]

# COMMAND ----------

print(last_processed_time_loans)

# COMMAND ----------

last_loaded_time_loans=spark.read.table("db_projects.dev.wateremark").filter("process_name='Ingest transactions'").select("watermark_time").collect()[0][0]

# COMMAND ----------

print(last_loaded_time_loans)

# COMMAND ----------

def readBronze_cust():
    from pyspark.sql.functions import sum, expr,col
    return(
    spark.read.table("db_projects.dev.customer_driver_bz").filter(
    col("IngestedDate").between(last_processed_time_cusdri, last_loaded_time_cusdri)
    )
    )
def readBronze_loans():
    from pyspark.sql.functions import sum, expr,col
    return(
    spark.read.table("db_projects.dev.trasactions_bz").filter(
    col("IngestedDate").between(last_processed_time_cusdri, last_loaded_time_cusdri)
    )
    )
def join_df(customers_driverDf,TransactionsDf):
    return(
        customers_driverDf.alias("cd").join(
        TransactionsDf.alias("t"),
        customers_driverDf["customer_id"] == TransactionsDf["customer_id"],
        how="inner"
    ).select('cd.date','cd.customer_id','payment_period','loan_amount','currency_type','evaluation_chan','interest_rate','monthly_salary','health_score','current_debt','category','risky_customer_flag')
    )
def aggregate_upsert(feature_loan_txnDf):
    # Step 2: Check if the target table exists
    try:
        spark.sql("desc db_projects.dev.feature_loan_txn")
        table_exists = True
    except:
        table_exists = False

    # Step 3: Handle insert or update logic
    if not table_exists:
        # If table does not exist, create it
        feature_loan_txnDf.write.mode("overwrite").saveAsTable("db_projects.dev.feature_loan_txn")
    else:
        # Create a temporary view
        feature_loan_txnDf.createOrReplaceTempView("feature_loan_txn_df_temp_view")
        # Perform the merge
        merge_statement = """
        MERGE INTO db_projects.dev.feature_loan_txn t
        USING feature_loan_txn_df_temp_view s
        ON s.customer_id = t.customer_id and
           s.date=t.date
        WHEN MATCHED THEN
            UPDATE SET t.payment_period=s.payment_period,
                        t.loan_amount=s.loan_amount,
                        t.currency_type=s.currency_type,
                        t.evaluation_chan=s.evaluation_chan,
                        t.interest_rate=s.interest_rate,
                        t.monthly_salary=s.monthly_salary,
                        t.health_score=s.health_score,
                        t.current_debt=s.current_debt,
                        t.category=s.category,
                        t.risky_customer_flag=s.risky_customer_flag
        WHEN NOT MATCHED THEN
            INSERT *
        """
        merge_result = spark.sql(merge_statement)
try:
    def process():
        print(f"\nStarting Silver Stream...", end='')
        customers_driverDf=readBronze_cust()
        TransactionsDf=readBronze_loans()
        feature_loan_txnDf=join_df(customers_driverDf,TransactionsDf)
        aggregate_upsert(feature_loan_txnDf)
        spark.sql("""update db_projects.dev.wateremark_silver
                        set watermark_time=current_timestamp()
                        where process_name='transform customer_drivers'""")
        spark.sql("""update db_projects.dev.wateremark_silver
                        set watermark_time=current_timestamp()
                        where process_name='transform transactions'""")
        print("Done")
        audit(feature_loan_txnDf,'db_projects.dev.feature_loan_txn')
    process()
except Exception as e:
    error_message=f"Error:{e}"
    error_function(error_message)
            
        

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_projects.dev.feature_loan_txn
