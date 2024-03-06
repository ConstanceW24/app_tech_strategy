import datetime
from pyspark.sql import SparkSession


spark= SparkSession.builder.getOrCreate()

def eventhub_stream_write(stream_df, ingress_config):
    """
    Writes the streaming dataframe into Azure eventhub topic 
    required writer/sink details are passed as config
    Parameters
    ----------
    ingress_config: Required arguments in the form of key-value pairs/options
                for reading from a streaming source
    
    Return
    ------
    None
    
    """
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


    username = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-eventhub-username")
    password = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-eventhub-password")
    bootstrapserver = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-eventhub-bootstrapserver")

    jaas_config= "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(username, password)

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