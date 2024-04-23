import pyspark
from pyspark.sql import SparkSession
from common_utils import read_write_utils
import traceback
spark= SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

def cobol_read(ingress_config):
    
    """
    
    Parameters
    ----------
    ingress_config: Required arguments in the form of key-value pairs/options
                for reading from a cobol source
    
    Return
    ------
    Returns the dataframe
    
    """
    try:
        # Batch Processing / Stream Processing check 
        raw_df = spark.read.format("za.co.absa.cobrix.spark.cobol.source")

        # Options 
        raw_df = read_write_utils.apply_options(ingress_config, raw_df)
        
        # Handling delta format 
        if str(ingress_config["data"]["inputFile"].get("options",{}).get("path","")).lower() not in ( "", "none"):
            raw_df = raw_df.load()
        else:
            # All Non delta table formats can be accessed through load()
            raw_df = raw_df.load(ingress_config["source"]["driver"]["path"])

        return raw_df
    
    except Exception as e:
        err_msg = f"Encountered exception while reading from Cobol - {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)
