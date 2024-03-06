import json
from databricks_api import DatabricksAPI
from pyspark.sql import SparkSession
from secrets_utils.secrets_manager import get_secrets, get_secrets_param
import os

spark= SparkSession.builder.getOrCreate()

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        try:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        except Exception as e:
            print(f'Exception Occured : {e}')
            dbutils = None
    return dbutils

dbutils = get_dbutils(spark)

def authenticate_db():
    """
    authenticate_db
    This fucntion is used to authenticate databricks using host/token in order to get the existing run/job details.
    Parameter:
        -json config
    Return:
        It returns a db object, which is used to access databricks api.
    """
    if(os.getenv('platform')=='azure'):
        db = DatabricksAPI(
            host= dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-databricks-host-name"),
            token=dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-databricks-token-value")
        )
    if(os.getenv('platform')=='aws'):
        db = DatabricksAPI(
            host= get_secrets_param("ane")['host'],
            token= get_secrets_param("ane")['token']
        )
    return db

def run_job(db, job_id, job_run_id):
    runid = db.jobs.run_now(
    job_id=job_id,
    python_named_params = {"job_run_id": job_run_id}  
  )
    return runid

def get_current_job_runid():
    context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    context = json.loads(context_str)
    run_id = context.get('tags', {}).get('multitaskParentRunId', None)  
    print(f"The current DP Job runId is {run_id}") 
    return run_id

def get_current_job_name():
    context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    context = json.loads(context_str)
    job_name = context.get('tags', {}).get('jobName', None)
    print(f"The current DP Job is {job_name}")
    return job_name