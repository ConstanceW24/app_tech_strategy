import datetime
import traceback
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from common_utils import utils as utils,  read_write_utils

spark= SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")


def bq_data_write(stream_df, egress_config, process_name = "Ingestion"):


    """
    Writes the streaming dataframe into bigquery 
    Parameters
    ----------
    stream_df: Input stream dataframe to be written into S3
    egress_config: required arguments in the form of key-value pairs/options
                for writing into a Bigquery

    .format("bigquery")
    .option("temporaryGcsBucket",egress_config["target"]["options"]["temporaryGcsBucket"])\
    .option('table',egress_config["target"]["options"]["table"])\


    Return
    ------
    Returns the streaming dataframe

    """
    def foreach_batch_function(df, epoch_id):
        df = df.write.format("bigquery")
        df = read_write_utils.apply_write_options(egress_config, df)
        df.save()

    try:
        # Apply no of partition
        stream_df =  read_write_utils.get_partition_df(egress_config, stream_df)

        # Write 
        stream_df =  read_write_utils.write_process_check(egress_config, stream_df)
        
        if str(egress_config["data"]["inputFile"].get('batchFlag','none')).lower() != 'true':
            stream_df = stream_df.foreachBatch(foreach_batch_function)

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
        err_msg = f"Encountered exception while writing to Bigquery - {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)