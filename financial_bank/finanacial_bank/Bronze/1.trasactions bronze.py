# Databricks notebook source
# MAGIC %run "/Workspace/setup/0.commonvar"

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Audit/Audit_to_Table_func_succes"

# COMMAND ----------

# MAGIC %run "/Workspace/Utils/Logging/2.Error Handling Code Table"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

watermark=spark.read.table("db_projects.dev.wateremark").filter("process_name='Ingest transactions'").select("watermark_time").collect()[0][0]

# COMMAND ----------

print(watermark)

# COMMAND ----------

def getSchema():
    schema="""date string,
            customerId string,
            paymentPeriod integer,
            loanAmount double,
            currencyType string,
            evaluationChannel string,
            interest_rate double
            """
    return schema

def readFile(file_schema):
    from pyspark.sql.functions import input_file_name,to_date,date_format
    return (
            spark.read.format("csv")
            .option("header",True)
            .schema(file_schema)
            .option("modifiedAfter", watermark)
            .load(f"{base_data_dir}/bank/trasactions")
            .withColumn("date",date_format(to_date("date", "dd/MM/yyyy"), "yyyy-MM-dd"))
            .withColumnRenamed("customerId","customer_id")
            .withColumnRenamed("paymentPeriod","payment_period")
            .withColumnRenamed("loanAmount","loan_amount")
            .withColumnRenamed("currencyType","currency_type")
            .withColumnRenamed("evaluationChannel","evaluation_chan")
            .withColumn("IngestedDate",current_timestamp())
            .withColumn("InputFile", input_file_name())
        )
try:
    def process():
        print(f"\nStarting Bronze Stream...", end="")
        file_schema=getSchema()
        trasactionsDF = readFile(file_schema)
        trasactionsDF.write.mode("append").saveAsTable("db_projects.dev.trasactions_bz")
        spark.sql("""update db_projects.dev.wateremark
                    set watermark_time=current_timestamp()
                    where process_name='Ingest transactions'""")
        print("Done")
        audit(trasactionsDF,'db_projects.dev.trasactions_bz')
    process()
except Exception as e:
    error_message=f"Error:{e}"
    error_function(error_message)
