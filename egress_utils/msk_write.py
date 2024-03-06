import datetime
from secrets_utils.secrets_manager import get_secrets
from pyspark.sql import SparkSession


spark= SparkSession.builder.getOrCreate()

def msk_stream_write(stream_df, ingress_config):
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

    secrets_val=get_secrets()
    bootstrapserver = secrets_val["ccs-dp-msk-bootstrapserver"]


    kwargs=ingress_config["target"]["options"]
    stream_df = stream_df.writeStream\
                            .format("kafka")\
                            .option("kafka.bootstrap.servers",bootstrapserver)
    if len(kwargs) > 0:
        for k,v in kwargs.items():
            stream_df = stream_df.option(k,v)

    if ingress_config["target"]["targetTrigger"] !='once':
        stream_df=stream_df.trigger(processingTime=ingress_config["target"]["targetTrigger"])
    else:
        stream_df=stream_df.trigger(once=True)

    stream_df = stream_df.start()

    return stream_df