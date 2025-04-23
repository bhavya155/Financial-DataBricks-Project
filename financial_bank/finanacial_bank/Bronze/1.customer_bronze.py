# Databricks notebook source
# MAGIC %run "/Workspace/setup/0.commonvar"

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Audit/Audit_to_Table_func_succes"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Logging/2.Error Handling Code Table"
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

watermark=spark.read.table("db_projects.dev.wateremark").filter("process_name='Ingest customers'").select("watermark_time").collect()[0][0]

# COMMAND ----------

print(watermark)

# COMMAND ----------

def getSchema():
    schema="""customerId string,
        firstName string,
        lastName string,
        phone string,
        email string,
        gender string,
        address string,
        is_active string
            """
    return schema

def readFile(file_schema):
    from pyspark.sql.functions import input_file_name
    return (
            spark.read.format("csv")
            .option("header",True)
            .schema(file_schema)
            .option("modifiedAfter", watermark)
            .load(f"{base_data_dir}/bank/customer/")
            .withColumnRenamed("customerId","customer_id")
            .withColumnRenamed("firstName","first_name")
            .withColumnRenamed("lastName","last_name")
            .withColumnRenamed("phone","phone_number")
            .withColumnRenamed("address","customer_address")
            .withColumnRenamed("is_active","active_status")
            .withColumn("IngestedDate",current_timestamp())
            .withColumn("InputFile", input_file_name())
        )
try:
    def process():
        print(f"\nStarting Bronze Stream...", end="")
        file_schema=getSchema()
        customersDF = readFile(file_schema)
        customersDF.dropDuplicates().write.mode("append").saveAsTable("db_projects.dev.customers_bz")
        spark.sql("""update db_projects.dev.wateremark
                    set watermark_time=current_timestamp()
                    where process_name='Ingest customers'""")
        print("Done")
        audit(customersDF,'db_projects.dev.customers_bz')
    process()
except Exception as e:
    error_message=f"Error:{e}"
    error_function(error_message)
