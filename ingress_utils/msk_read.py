import datetime
import os
import json, traceback
from secrets_utils.secrets_manager import get_secrets, get_secrets_param
from pyspark.sql import SparkSession
from pyspark.sql.functions import (array, coalesce, col, count, current_date,
                                   datediff, explode, expr, length, lit, lower,
                                   regexp_replace, split, substring, sum,
                                   to_date, to_timestamp, trim, udf, when,
                                   from_json,col,split,input_file_name)
spark= SparkSession.builder.getOrCreate()

def read_mskstream(ingress_config):
    """
    Reads stream data from Managed streaming kafka topic
    into a streaming dataframe
    
    Parameters
    ----------
    ingress_config: Required arguments in the form of key-value pairs/options
                for reading from a streaming source
    
    Return
    ------
    Returns the streaming dataframe
    
    """
    def get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils  
    _ = get_dbutils(spark)
    secrets_val=get_secrets()
    bootstrapserver = secrets_val["ccs-dp-msk-bootstrapserver"]

    kwargs=ingress_config["data"]["inputFile"]["options"]
    raw_df = (spark
                .readStream
                .format(ingress_config["source"]["driver"]["format"])\
                .option("kafka.bootstrap.servers",bootstrapserver)
                )
    if len(kwargs) > 0:
        for k,v in kwargs.items():
            raw_df = raw_df.option(k,v)
    raw_df = raw_df.load()
    return raw_df

def read_msk_kafkastream(ingress_config):
    """
    Reads stream data from AWS Managed streaming kafka topic
    into a streaming dataframe
    
    Parameters
    ----------
    ingress_config: Required arguments in the form of key-value pairs/options
                for reading from a streaming source
    
    Return
    ------
    Returns the streaming dataframe
    
    """
    try:
        def get_dbutils(spark):
            try:
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
            except ImportError:
                import IPython
                dbutils = IPython.get_ipython().user_ns["dbutils"]
            return dbutils  
        dbutils = get_dbutils(spark)

        if(os.getenv('platform')=='azure'):
            schema_registry_subject = ingress_config["data"]["inputFile"]["options"]["schemaRegistrySubject"]
            schema_registry_address = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-kafka-schemaRegistryAddress")
            bootstrapserver = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-kafka-bootstrapserver")
        if(os.getenv('platform')=='aws'):
            schema_registry_subject = ingress_config["data"]["inputFile"]["options"]["schemaRegistrySubject"]
            secret_string = get_secrets_param("msi")
            schema_registry_address = secret_string["mskschemaregistry"]
            bootstrapserver = secret_string["msi_kafka_mskbootstrapserver"]

        kwargs=ingress_config["data"]["inputFile"]["options"]
        raw_df = (spark
                    .readStream
                    .format(ingress_config["source"]["driver"]["format"])\
                    .option("kafka.bootstrap.servers",bootstrapserver)
                    )
        if len(kwargs) > 0:
            for k,v in kwargs.items():
                raw_df = raw_df.option(k,v)
        raw_df = raw_df.load()
        return raw_df,schema_registry_address,schema_registry_subject
    except Exception as e:
        err_msg = f"Encountered exception while writing to Blob - {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)