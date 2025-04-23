# Databricks notebook source
import json,datetime,uuid

def error_function(error_message):
    notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    log_Id=str(uuid.uuid4())
    jobId = notebook_info["tags"]["jobId"]
    runId = notebook_info["tags"]["jobRunId"]
    taskRunId = notebook_info["tags"]["runId"]
    startTime = datetime.datetime.fromtimestamp(int(notebook_info["tags"]["startTimestamp"]) / 1000)
    endTime = datetime.datetime.fromtimestamp(int(notebook_info["tags"]["endTimestamp"]) / 1000)
    status='Failed'
    # to logging
    insert_sql = f"""INSERT INTO db_projects.dev.job_errors VALUES ('{log_Id}',{jobId},{runId},{taskRunId},"{error_message}",'{startTime}','{endTime}')"""
    spark.sql(insert_sql)
    print("Error Data inserted successfully.")
    # to audit
    insert_audit = f"""
    INSERT INTO db_projects.dev.audit_table (job_id, job_run_id, taskRunId, status, startTime, endTime)
    VALUES ({jobId}, {runId},'{taskRunId}' ,'{status}' ,'{startTime}', '{endTime}')
    """
    spark.sql(insert_audit)
