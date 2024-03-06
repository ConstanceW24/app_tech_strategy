import datetime
import traceback
from pyspark.sql import SparkSession
from common_utils import utils as utils,  read_write_utils

spark= SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")


def delta_data_write(stream_df, egress_config, process_name = "Ingestion"):
    """
    Writes the streaming dataframe into delta in given format like csv/parquet
    required writer/sink details are passed as kwargs
    
    Parameters
    ----------
    stream_df: Input stream dataframe to be written into S3
    ingress_config: required arguments in the form of key-value pairs/options
                for writing into a streaming sink/S3 
    
    Return
    ------
    Returns the streaming dataframe
    
    """
    try:
        # Apply no of partition
        stream_df =  read_write_utils.get_partition_df(egress_config, stream_df)

        # Write 
        stream_df =  read_write_utils.write_process_check(egress_config, stream_df)
        
        # Apply Options
        stream_df = read_write_utils.apply_write_options(egress_config, stream_df)

        # Batch Mode
        if str(egress_config["data"]["inputFile"].get('batchFlag','none')).lower() == 'true':
            read_write_utils.check_for_delta(egress_config, stream_df)

        # Stream Mode
        else:
            stream_df = read_write_utils.apply_trigger_option(egress_config, stream_df)
            stream_df = stream_df.outputMode("append").queryName(process_name)

            if 'delta' in egress_config["target"].get("options","").get("format","") and str(egress_config["target"]["options"].get("table", "")).lower() not in ( "", "none"):
                stream_final_df = stream_df.table(egress_config["target"]["options"]["table"])
            else:
                stream_final_df = stream_df.start()

            read_write_utils.check_target_trigger(egress_config, stream_final_df)           

    except Exception as e:
        err_msg = f"Encountered exception while writing to Blob - {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)

