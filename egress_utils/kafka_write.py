# Databricks notebook source
# DBTITLE 1,Import Statements
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (array, coalesce, col, count, current_date,
                                   datediff, explode, expr, length, lit, lower,
                                   regexp_replace, split, substring, sum,
                                   to_date, to_timestamp, trim, udf, when,
                                   from_json,col,split,input_file_name)
import traceback

spark= SparkSession.builder.getOrCreate()

def kafka_stream_write(stream_df, ingress_config):

    """
    Writes the streaming dataframe into Kafka/ AWS MSK
    required writer/sink details are passed as kwargs

    Parameters
    ----------
    stream_df: Input stream dataframe to be written into Kafka/MSK
    ingress_config: Required arguments in the form of key-value pairs/options
                for writing into a streaming sink/MSK/Kafka

    Return
    ------
    None; Only writes the stream into sink

    """
    try:
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


        confluentapikey = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-kafka-confluentapikey")
        confluentsecret = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-kafka-confluentsecret")
        bootstrapserver = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-kafka-bootstrapserver")

        jaas_config= "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentapikey, confluentsecret)

        kwargs=ingress_config["target"]["options"]
        stream_df = stream_df.writeStream\
                                .format("kafka")\
                                .option("kafka.bootstrap.servers",bootstrapserver)\
                                .option("kafka.sasl.jaas.config",jaas_config)
        if len(kwargs) > 0:
            for k,v in kwargs.items():
                stream_df = stream_df.option(k,v)

        if ingress_config["target"]["targetTrigger"] !='once':
            stream_df=stream_df.trigger(processingTime=ingress_config["target"]["targetTrigger"])
        else:
            stream_df=stream_df.trigger(once=True)

        stream_df = stream_df.start()

        return stream_df
    except Exception as e:
        err_msg = f"Encountered exception while writing to Blob - {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)

