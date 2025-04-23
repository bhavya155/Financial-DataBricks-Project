# Databricks notebook source
# MAGIC %run "/Workspace/setup/0.commonvar"

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Audit/Audit_to_Table_func_succes"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Logging/2.Error Handling Code Table"
# MAGIC

# COMMAND ----------

last_processed_time_cus=spark.read.table("db_projects.dev.wateremark_silver").filter("process_name=='transform customers'").select("watermark_time").collect()[0][0]

# COMMAND ----------

last_loaded_time_cus=spark.read.table("db_projects.dev.wateremark").filter("process_name='Ingest customers'").select("watermark_time").collect()[0][0]

# COMMAND ----------

print(last_processed_time_cus)


# COMMAND ----------

print(last_loaded_time_cus)

# COMMAND ----------

def readBronze():
    from pyspark.sql.functions import sum, expr,col
    return(
    spark.read.table("db_projects.dev.customers_bz").filter(
    col("IngestedDate").between(last_processed_time_cus, last_loaded_time_cus)
)
    )
def aggregate_upsert(customersDF):
    # Step 2: Check if the target table exists
    try:
        spark.sql("desc db_projects.dev.customers_master")
        table_exists = True
    except:
        table_exists = False

    # Step 3: Handle insert or update logic
    if not table_exists:
        # If table does not exist, create it
        customersDF.write.mode("overwrite").saveAsTable("db_projects.dev.customers_master")
    else:
        # Create a temporary view
        customersDF.createOrReplaceTempView("customers_master_df_temp_view")
        # Perform the merge
        merge_statement = """
        MERGE INTO db_projects.dev.customers_master t
        USING customers_master_df_temp_view s
        ON s.customer_id = t.customer_id
        WHEN MATCHED THEN
            UPDATE SET t.first_name = s.first_name ,
                    t.last_name = s.last_name ,
                    t.phone_number=s.phone_number,
                    t.email=s.email,
                    t.gender=s.gender,
                    t.customer_address=s.customer_address,
                    t.active_status=s.active_status,
                    t.IngestedDate=s.IngestedDate,
                    t.InputFile =s.InputFile
        WHEN NOT MATCHED THEN
            INSERT *
        """
        merge_result = spark.sql(merge_statement)
try:
    def process():
        print(f"\nStarting Silver Stream...", end='')
        customersDF=readBronze()
        aggregate_upsert(customersDF)
        spark.sql("""update db_projects.dev.wateremark_silver
                        set watermark_time=current_timestamp()
                        where process_name='transform customers'""")
        print("Done")
        audit(customersDF,'db_projects.dev.customers_master')
    process()
except Exception as e:
    error_message=f"Error:{e}"
    error_function(error_message)
            
        
