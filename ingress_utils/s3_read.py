from pyspark.sql import SparkSession
import traceback
from common_utils import read_write_utils


spark= SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

def s3_data_read(ingress_config):
    
    """
    Reads stream data from S3 of any type - csv/parquet etc.
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
        
        # Batch Processing / Stream Processing check 
        raw_df = read_write_utils.read_process_check(ingress_config)
        
        # Options 
        raw_df = read_write_utils.apply_options(ingress_config, raw_df)

        #Integrated in options  "badRecordsPath" : "/mnt/path/badrecord/{current_timestamp}"

        # Handling delta format 
        if 'delta' in ingress_config["data"]["inputFile"]["inputFileFormat"] :
            raw_df = read_write_utils.delta_format_input(ingress_config, raw_df)
        else:
            # All Non delta table formats can be accessed through load()
            if str(ingress_config["data"]["inputFile"].get("options",{}).get("path","")).lower() not in ( "", "none"):
                raw_df = raw_df.load()
            else:
                # All Non delta table formats can be accessed through load()
                raw_df = raw_df.load(ingress_config["source"]["driver"]["path"])

        #checking and performing column mapping            
        raw_df = read_write_utils.col_mapping(ingress_config,raw_df)

        return raw_df
    
    except Exception as e:
        err_msg = f"Encountered exception while reading from BLOB - {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)