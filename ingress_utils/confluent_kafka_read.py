# Databricks notebook source
# DBTITLE 1,Import Statements
import datetime
from pyspark.sql import SparkSession
import traceback
from pyspark.sql.functions import (array, coalesce, col, count, current_date,
                                   datediff, explode, expr, length, lit, lower,
                                   regexp_replace, split, substring, sum,
                                   to_date, to_timestamp, trim, udf, when,
                                   from_json,col,split,input_file_name)
spark= SparkSession.builder.getOrCreate()

def read_kafkastream(ingress_config):
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

    
        confluentapikey = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-kafka-confluentapikey")
        confluentsecret = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-kafka-confluentsecret")
        bootstrapserver = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-kafka-bootstrapserver")

        jaas_config= "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentapikey, confluentsecret)

        kwargs=ingress_config["data"]["inputFile"]["options"]
        raw_df = (spark
                    .readStream
                    .format(ingress_config["source"]["driver"]["format"])\
                    .option("kafka.bootstrap.servers",bootstrapserver)\
                    .option("kafka.sasl.jaas.config",jaas_config)
                    )
        if len(kwargs) > 0:
            for k,v in kwargs.items():
                raw_df = raw_df.option(k,v)
        raw_df = raw_df.load()
        return raw_df
    except Exception as e:
        err_msg = f"Encountered exception while reading from KAFKA - {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)