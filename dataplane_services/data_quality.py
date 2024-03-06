"""Data Quality
This script performs data quality checks on the source streaming data.
This script can read the data from various source system as defined in the configuration
and stores the data into spark dataframe and build all the required
data quality validations to perform on the event payload.
It receives the data quality config as parameter from the data_initialization file,
iterates the rules list from the configuration and perform data quality check and handle the exception gracefuly
and write the validated data to specified target location for next service.
This script requires that `.whl` be installed within the EMR/Databricks Cluster you are running this script in.
This script requires to import the common_utils,ingress_utils,egress_utils files to refer the common functions inside the script.
This file can also be imported as a module and contains the following
functions:
    * data_quality - Performs DQ check and writes the spark dataframe to the target location defined in the configuration
"""


import sys, traceback
from datetime import datetime
from copy import deepcopy
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import (array, coalesce, col, count, current_date,
                                   datediff, explode, expr, length, lit, lower,
                                   regexp_replace, split, substring, sum,
                                   to_date, to_timestamp, trim, udf, when)
from common_utils.batch_writer import batch_writer
from data_quality_validations import *
from common_utils import utils, stream_process_utils as stream
from os.path import dirname

spark= SparkSession.builder.getOrCreate()


def stream_data_quality_check(df_stream,ingress_config):
    source_file_name    = ingress_config["data"]["inputFile"]["fileName"].replace("/","")
    process_name        = f"DataQuality-{source_file_name}"

    if ingress_config['target'].get('rejectOptions','') != '':
        for i in ingress_config['rules']:
            i['reject_handling'] = 'false'

    for idx,i in enumerate(ingress_config['rules']):
        print(idx)
        print(i)   #Iterating the rules from Data Quality Config
        if i.get('rule_parser', '').lower() == 'pyspark_foreachbatch':
            print('Running checks using foreachbatch_transformation')
            stream.foreachbatch_dq_checks(ingress_config['rules'][idx:], df_stream ,ingress_config)
            return None
        else:
            valid_check = f'''{i['validation_name']}(df_stream,i)'''
            df_stream=eval(valid_check)


    write_summary = False

    if ingress_config['target'].get('rejectOptions',None) != None:
        df_stream, rejected_df = stream.get_rejected_records(df_stream,ingress_config)
        reject_data_config, reject_summary_config = stream.generate_reject_configs(ingress_config)
        if rejected_df is not None:
            stream.file_storage_write_stream(f'{process_name}_rejected', rejected_df, reject_data_config)
        write_summary = True

    if write_summary:
        print("Generating Summary from rejected records")
        stream.foreachbatch_dq_checks([], rejected_df ,reject_summary_config, summarize = True)

    return df_stream

def data_quality(ingress_config):
    """Gets the data quality config and read the payload data from source location, performs Data quality validations,
     handle exception if DQ fails and write the validated payload data to target location for next service.
    Parameters
    ----------
    df_config : json
        The data quality configuration
    Conditions
    ----------
    validated_config : bool
        A condition to display the status of config validation returned from comutil.parse_and_validate_config().
    ingress_config["source"]["driver"]["SourceType"] : string
        A condition to check the sourcetype and invoke the respective function from S3Stream/KafkaStream class files to read the data event.
    ingress_config["target"]["targetType"] : string
        A condition to check the targetType and invoke the respective function from S3Stream/KafkaStream class files to write the data event.
    rule_parser: string
        This script supports both pyspark and spark sql language to perform DQ validation, If rule_parser is SparkSQL, it reads the
        rule_overrite value and constructs the sql query and create the tempview from dataframe and execute the sql query and store the results
        to dataframe, if the rule_parser is pyspark then this script builds all the required validation script using the builtin pyspark function
        and validates data in the dataframe.
    exception_handling: string
        If data quality validation fails, this script handle exception with the following conditions.
            1. Default: if the DQ fails, it will replace the failure with Default value
            2. Reject: if the DQ fails, it will reject those records
            3. Abort: if the DQ fails, it will abort the entire process
    Exceptions
    ----------
    Exceptions are being raised during the validation of the configurations
    and throughout the script and throws the exception and stop the process.
    # Exceptions will be routed to ERS and all the state changes will be transitioned to FSM once integration is completed.
    Returns
    -------
        None
    """
    #Fetching source file details and creating view for future use
    source_file_name    = ingress_config["data"]["inputFile"]["fileName"].replace("/","")
    process_name        = f"DataQuality-{source_file_name}"


    try:   
        ############Fetching source file details and creating view for future use
        df_stream = stream.source_df_readstream(ingress_config)


        ############ Write the Stream into S3 CSV ##########
        df_stream = stream_data_quality_check(df_stream,ingress_config)
        if df_stream is not None:
            stream.file_storage_write_stream(process_name, df_stream, ingress_config)


    except Exception as e:
        err_msg = f"Encountered exception while starting the Data Quality Stream process- {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)