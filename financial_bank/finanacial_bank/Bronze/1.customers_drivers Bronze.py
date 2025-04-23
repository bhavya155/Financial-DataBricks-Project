# Databricks notebook source
# MAGIC %run "/Workspace/setup/0.commonvar"

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Audit/Audit_to_Table_func_succes"

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Logging/2.Error Handling Code Table"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

watermark=spark.read.table("db_projects.dev.wateremark").filter("process_name='Ingest customer_drivers'").select("watermark_time").collect()[0][0]

# COMMAND ----------

print(watermark)

# COMMAND ----------

def getSchema():
    schema="""date date,
            customerId string,
            monthly_salary integer,
            health_score integer,
            current_debt double,
            category string
            """
    return schema

def readFile(file_schema):
    from pyspark.sql.functions import col, lit, when, current_timestamp, input_file_name,coalesce
    return (
            spark.read.format("csv")
            .option("header",True)
            .schema(file_schema)
            .option("modifiedAfter", watermark)
            .load(f"{base_data_dir}/bank/customer_driver/")
            .withColumnRenamed("customerId","customer_id")
            .withColumn("monthly_salary",coalesce(col("monthly_salary"), lit(1500)))
            .withColumn("health_score", coalesce(col("health_score"), lit(100)))
            .withColumn("current_debt", coalesce(col("current_debt"), lit(0)))
            .withColumn("category", coalesce(col("category"), lit("OTHERS")))
            .withColumn("risky_customer_flag", when(col("health_score") < 100, True).otherwise(False))
            .withColumn("IngestedDate",current_timestamp())
            .withColumn("InputFile", input_file_name())
        )
try:
    def process():
        print(f"\nStarting Bronze Stream...", end="")
        file_schema=getSchema()
        customer_driverDF = readFile(file_schema)
        customer_driverDF.write.mode("append").saveAsTable("db_projects.dev.customer_driver_bz")
        spark.sql("""update db_projects.dev.wateremark
                    set watermark_time=current_timestamp()
                    where process_name='Ingest customer_drivers'""")
        print("Done")
        audit(customer_driverDF,'db_projects.dev.customer_driver_bz')
    process()
except Exception as e:
    error_message=f"Error:{e}"
    error_function(error_message)
