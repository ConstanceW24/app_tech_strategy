from pyspark.sql import SparkSession
import traceback
from common_utils import read_write_utils

spark= SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")
from secrets_utils.secrets_manager import get_secrets_param
from common_utils.utils import get_dbutils , apply_request_header



def db_read(ingress_config):
    
    """
    Reads stream data from sql database into a streaming dataframe.
    
    Parameters
    ----------
    ingress_config: Required arguments in the form of key-value pairs/options
                for reading from a streaming source
    
    Return
    ------
    Returns the streaming dataframe
    
    """
    try:
        
        #Batch read from sqldb
        raw_df = spark.read.format("jdbc")

        options_dict = read_write_utils.get_credential_details(ingress_config, ingress_config["data"]["inputFile"].get("options",{}))

        kwargs = ingress_config["data"]["inputFile"].get("options",{})
        options_dict.update(kwargs)
        
        cred_dict = read_write_utils.map_connection_options(options_dict)

        print(cred_dict)
        raw_df = read_write_utils.apply_to_dataframe(raw_df, cred_dict)
        
        #checking and performing reject handling            
        raw_df = read_write_utils.rej_handling(ingress_config, raw_df)

        raw_df = raw_df.load()

        #checking and performing column mapping            
        raw_df = read_write_utils.col_mapping(ingress_config, raw_df)

        return raw_df
    
    except Exception as e:
        err_msg = f"Encountered exception while reading from BLOB - {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)
