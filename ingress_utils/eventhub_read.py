import datetime
from pyspark.sql import SparkSession

spark= SparkSession.builder.getOrCreate()


def read_eventhub(ingress_config):
    """
    Reads stream data from azure eventhub
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
    dbutils = get_dbutils(spark)

    
    username = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-eventhub-username")
    password = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-eventhub-password")
    bootstrapserver = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-eventhub-bootstrapserver")

    jaas_config= "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(username, password)

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