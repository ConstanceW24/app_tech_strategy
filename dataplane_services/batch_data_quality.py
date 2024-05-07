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
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from common_utils import utils, batch_process_utils as batch
from data_quality_validations import *
from datetime import datetime


spark= SparkSession.builder.getOrCreate()


def data_quality_check(df,ingress_config):
    source_file_name    = ingress_config["data"]["inputFile"]["fileName"].replace("/","")
    process_name        = f"DataQuality-{source_file_name}"

    if ingress_config['target'].get('rejectOptions', None) != None :
        for i in ingress_config['rules']:
            i['reject_handling'] = 'true'

    print(ingress_config['rules'])
    for idx,i in enumerate(ingress_config['rules']):
        print(idx)
        print(i)   #Iterating the rules from Data Quality Config
        
        valid_check = f'''{i['validation_name']}(df,i)'''
        df = eval(valid_check)


    write_summary = False

    if ingress_config['target'].get('rejectOptions',None) != None or ingress_config['target'].get('validationLogOptions',None) != None:
        df, rejected_df, logged_df = batch.get_rejected_records(df, ingress_config)
        reject_data_config, log_config, reject_summary_config = batch.generate_reject_configs(ingress_config)
        if rejected_df is not None:
            write_summary = True
            if reject_data_config != {}:
                batch.batch_writer(rejected_df, reject_data_config, spark)

            
            if log_config != {}:
                print("Generating Rejection Summary for validation failed records")
                batch.log_dq_fail(rejected_df, log_config, 'Reject')
    
        if logged_df is not None and log_config != {}:
            print("Generating logging for validation failed records")
            batch.log_dq_fail(logged_df, log_config)

    if write_summary and reject_summary_config != {}:
        print("Generating Summary from rejected records")
        batch.write_summary(rejected_df, reject_summary_config)

    return df

def batch_data_quality(ingress_config):
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
        df_stream = batch.source_df_read(ingress_config, process_name)


        ############ Write the Stream into S3 CSV ##########
        df_stream = data_quality_check(df_stream,ingress_config)
        if df_stream is not None:
            batch.file_storage_write(process_name, df_stream, ingress_config)

    except Exception as e:
        err_msg = f"Encountered exception while starting the Data Ingestion Stream process- {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)
