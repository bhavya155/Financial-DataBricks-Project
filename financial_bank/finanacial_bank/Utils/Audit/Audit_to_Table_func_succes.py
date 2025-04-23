# Databricks notebook source
def audit(source,target):
    from pyspark.sql.functions import col
    import json,datetime
    rows_read=source.count()
    target_table=f"{target}"
    history_df = spark.sql(f"DESCRIBE HISTORY {target}")
    notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    jobId = notebook_info["tags"]["jobId"]
    runId = notebook_info["tags"]["jobRunId"]
    taskRunId = notebook_info["tags"]["runId"]
    startTime = datetime.datetime.fromtimestamp(int(notebook_info["tags"]["startTimestamp"]) / 1000)
    endTime = datetime.datetime.fromtimestamp(int(notebook_info["tags"]["endTimestamp"]) / 1000)
    status='Success'
    # Get the latest operation (last row in history)
    latest_operation = history_df.orderBy(col("version").desc()).limit(1).collect()[0]
    operation_metrics = latest_operation["operationMetrics"]
    operation=latest_operation["operation"]
    rows_written = int(operation_metrics.get("numOutputRows", 0))
    time_stamp=latest_operation["timestamp"]
    # Insert the data into the audit table
    insert_query = f"""
    INSERT INTO db_projects.dev.audit_table (job_id, job_run_id, taskRunId,rows_read, operation, status, rows_written,target, startTime,endTime, time_stamp)
    VALUES ({jobId}, {runId},'{taskRunId}', {rows_read}, '{operation}' ,'{status}', {rows_written},'{target_table}', '{startTime}', '{endTime}','{time_stamp}')
    """
    spark.sql(insert_query)

