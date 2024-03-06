"""Data Ingestion
This script performs data transfer from various source system as defined in the data ingestion configuration
to the target destination. It receives the config as parameter from  the Data_Initialization file, gets the respective source location
and destination for each event.

This script requires that `.whl` be installed within the EMR/Databricks
Cluster you are running this script in.

This script requires to import the common_utils,ingress_utils,egress_utils files to refer the common functions inside the script.

This file can also be imported as a module and contains the following

functions:
    * data_ingestion - writes the spark dataframe to the target location defined in the configuration
"""
import sys
import traceback
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from common_utils import utils, batch_process_utils as batch
from data_quality_validations import *

spark= SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")


def batch_data_ingestion(ingress_config):
    """Gets the data ingestion config and read the payload data from source location and write the payload data to target location.

    Parameters
    ----------
    ingress_config : json
        The data ingestion configuration

    Conditions
    ----------

    validated_config : bool
        A condition to display the status of config validation returned from comutil.parse_and_validate_config().
    ingress_config["source"]["driver"]["SourceType"] : string
        A condition to check the sourcetype and invoke the respective function from S3Stream/KafkaStream class files to read the data event.
    ingress_config["target"]["targetType"] : string
        A condition to check the targetType and invoke the respective function from S3Stream/KafkaStream class files to write the data event.

    Exceptions
    ----------
    Exceptions are being raised during the validation of the configurations
    and throughout the script and throws the exception and stop the process.

    # Exceptions will be routed to ERS and all the state changes will be transitioned to FSM once integration is completed.

    Returns
    -------
        Source Count & Target Count

    """

    source_file_name    = ingress_config["data"]["inputFile"]["fileName"].replace("/","")
    process_name        = f"DataIngestion-{source_file_name}"

    try:        
        ############## Read & Stream Files ##################
        source_df = batch.source_df_read(ingress_config, process_name)
        
        ############## Data Quality Validations ##################

        raw_df = source_df
        if ingress_config.get('dqrules', None) is not None :
            if ingress_config['dqrules'].get('rules', None) is not None:
                for i in ingress_config['dqrules']['rules']:
                    valid_check = f'''{i['validation_name']}(raw_df,i)'''
                    raw_df=eval(valid_check)

        ###########Write the Stream into destination or target sink ##########
        batch.file_storage_write(process_name, raw_df, ingress_config)

    except Exception as e:
        err_msg = f"Encountered exception while starting the Data Ingestion Stream process- {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)