# Databricks notebook source
# DBTITLE 1,Import Statements
import datetime
from pyspark.sql import SparkSession
import traceback
from common_utils import read_write_utils


def jdbc_write(stream_df, egress_config, process_name = "Persistence"):
    """
    Writes the streaming dataframe into sql database table.
    required writer/sink details are passed as config
    
    Parameters
    ----------
    stream_df: Input stream dataframe to be written into sql database
    egress_config: required arguments in the form of key-value pairs/options
                for writing into a streaming sink/sqldatabase 
    
    Return
    ------
    None
    
    """
    try:

        # Apply no of partition
        stream_df =  read_write_utils.get_partition_df(egress_config, stream_df)

        # Write 
        stream_df =  read_write_utils.write_process_check(egress_config, stream_df)
        
        stream_df = stream_df.format("jdbc")
        
        # Batch Mode
        if str(egress_config["data"]["inputFile"].get('batchFlag','none')).lower() == 'true':
            options_dict = read_write_utils.get_credential_details(egress_config, egress_config["target"].get("options",{}))
            kwargs = egress_config["target"].get("options",{})
            options_dict.update(kwargs)           
            cred_dict = read_write_utils.map_connection_options(options_dict)
            print(cred_dict)
            stream_df = read_write_utils.apply_to_dataframe(stream_df, cred_dict)
            stream_df.save()

        # Stream Mode
        else:

            def foreach_batch_function(stream_df, epoch_id):
                """Gets the streaming dataframe as mini batch dataframe with unique batch id
                Parameters
                ----------
                df : dataframe
                The data persistence configuration
                epoch_id: int
                Unique id for data deduplication
                Returns
                -------
                Boolean
                """
                
                stream_df = stream_df.write.format("jdbc")
                
                options_dict = read_write_utils.get_credential_details(egress_config, egress_config["target"].get("options",{}))
                kwargs = egress_config["target"].get("options",{})
                options_dict.update(kwargs)           
                cred_dict = read_write_utils.map_connection_options(options_dict)
                print(cred_dict)

                stream_df = read_write_utils.apply_to_dataframe(stream_df, cred_dict)
                stream_df.save()

            stream_df = stream_df.foreachBatch(foreach_batch_function)
            stream_df = read_write_utils.apply_trigger_option(egress_config, stream_df)
            stream_df = stream_df.outputMode("append").option("checkpointLocation", egress_config["target"]["options"]["checkpointLocation"])\
                        .queryName(process_name)
            
            stream_final_df = stream_df.start()

            read_write_utils.check_target_trigger(egress_config, stream_final_df)       
        
    except Exception as e:
        err_msg = f"Encountered exception while writing to S3 - {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)
