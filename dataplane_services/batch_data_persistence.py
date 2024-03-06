"""Data Persistence

This script performs persistence of the Transformed/Augmented event payload to various
target system as defined in the data persistence configuration.

It receives the config as parameter from the Data_Initialization file, gets the respective source location
and destination for each event.

This script requires that `.whl` be installed within the EMR/Databricks
Cluster you are running this script in.

This script requires to import the common_utils,ingress_utils,egress_utils files to refer the common functions inside the script.
This file can also be imported as a module and contains the following

functions:
    * data_persistance - writes the spark dataframe to the target location defined in the configuration
"""
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, lit, struct, concat, collect_list, concat_ws
from common_utils import utils
from ingress_utils import blob_read, s3_read, confluent_kafka_read, delta_read
from egress_utils import blob_write, jdbc_write, s3_write, api_write, delta_write, kafka_write, eventhub_write, msk_write
from egress_utils import snowflake_write as sf_write, redshift_write
from data_quality_validations import *
from common_utils.utils import get_dbutils
from dataplane_services.batch_data_quality import *
from dataplane_services.batch_data_transformation import run_predqchecks, load_dependents, rule_transformation
from copy import deepcopy

spark= SparkSession.builder.getOrCreate()


dbutils = get_dbutils(spark)

def egress_check_read(egress_config):
    final_df, final_raw_df, final_merged_df = None, None, None

    if egress_config["source"]["driver"]["SourceType"] =='s3':
        final_df = s3_read.s3_data_read(egress_config)

    if egress_config["source"]["driver"]["SourceType"] =='blob':
        final_df = blob_read.blob_data_read(egress_config)

    if egress_config["source"]["driver"]["SourceType"] =='delta':
        file_location = egress_config["source"]["driver"]["path"]
        egress_config["source"]["driver"]["path"] = file_location +"_raw"
        final_raw_df = delta_read.delta_stream_read(egress_config)
        #NEED TO CONFIRM THIS PART
        egress_config["source"]["driver"]["path"] = file_location +"_merged"
        final_merged_df = delta_read.delta_stream_read(egress_config)

        egress_config["source"]["driver"]["path"] = file_location
    
    return final_df, final_raw_df, final_merged_df

def check_tgttype_write(egress_config, final_df):
    if egress_config["target"]["targetType"] in ['kafka','msk','eventhub']:
        tgttype = egress_config["target"]["targetType"]
        final_df = final_df.select(to_json(struct("*")).alias("value"))
        if tgttype == 'kafka':
            kafka_write.kafka_stream_write(final_df, egress_config)
        elif tgttype == 'msk':
            msk_write.msk_stream_write(final_df, egress_config)
        else:
            eventhub_write.eventhub_stream_write(final_df, egress_config)
    return final_df

def batch_data_persistence(egress_config):

    """Gets the data persistence config and read the payload data from source location and write the payload data to target location.

    Parameters
    ----------
    egress_config : json
        The data persistence configuration

    Conditions
    ----------

    validated_config : bool
        A condition to display the status of config validation returned from comutil.parse_and_validate_config().
    egress_config["source"]["driver"]["SourceType"] : string
        A condition to check the sourcetype and invoke the respective function from S3Stream/KafkaStream class files to read the data event.
    egress_config["target"]["targetType"] : string
        A condition to check the targetType and invoke the respective function from S3Stream/KafkaStream class files to write the data event.

    Exceptions
    ----------
    Exceptions are being raised during the validation of the configurations
    and throughout the script and throws the exception and stop the process.

    # Exceptions will be routed to ERS and all the state changes will be transitioned to FSM once integration is completed.

    # planning to keep the secret manager details seperately and get all the secrets required for application
      and import the file and refer the secrets as variable in all the wrapper functions.

    Returns
    -------
        None

    """

    source_file_name    = egress_config["data"]["inputFile"]["fileName"].replace("/","")
    process_name        = f"DataPersistence-{source_file_name}"

    ############## Read & Stream Files ##################
    try:

        final_df, final_raw_df, final_merged_df = egress_check_read(egress_config)

        ############## Data Quality Validations ##################
        final_df = run_predqchecks( final_df, egress_config, source_file_name)

        ############## Reading the Dependent data ##################
        
        load_dependents(egress_config)

        ############## Iterating the Data Transformation Rules ##################
        final_df.createOrReplaceTempView(source_file_name)
        
        for i in egress_config['rules']:
            final_df = rule_transformation(i, source_file_name, final_df)


        if egress_config.get('dqrules', None)  is not None:
            if egress_config['dqrules'].get('postdtchecks','').lower() == 'true':
                dq_config = deepcopy(egress_config)
                dq_config['rules'] = dq_config['dqrules']['rules']
                final_df = data_quality_check(final_df, dq_config)

        ###########Write the Stream into destination or target sink ##########
        final_df = check_tgttype_write(egress_config, final_df)

        if egress_config["target"]["targetType"] == 'snowflake':

            sfoptions1 = dict(sfUrl=egress_config["target"]["options"]["sfUrl"],
                                sfUser=egress_config["target"]["options"]["sfUser"],
                                sfPassword=egress_config["target"]["options"]["sfPassword"],
                                sfDatabase=egress_config["target"]["options"]["sfDatabase"],
                                sfSchema=egress_config["target"]["options"]["sfSchema_tgt"],
                                sfWarehouse=egress_config["target"]["options"]["sfWarehouse"])
            sfoptions2 = dict(sfUrl=egress_config["target"]["options"]["sfUrl"],
                                sfUser=egress_config["target"]["options"]["sfUser"],
                                sfPassword=egress_config["target"]["options"]["sfPassword"],
                                sfDatabase=egress_config["target"]["options"]["sfDatabase"],
                                sfSchema=egress_config["target"]["options"]["sfSchema"],
                                sfWarehouse=egress_config["target"]["options"]["sfWarehouse"])

            raw_tbl = f"""{egress_config["target"]["options"]["dbtable"]}_raw"""
            sf_raw_tbl = f"""{egress_config["target"]["options"]["sfDatabase"]}.{egress_config["target"]["options"]["sfSchema_tgt"]}.{raw_tbl}"""
            sf_raw_stg_tbl= f"""{egress_config["target"]["options"]["sfDatabase"]}.{egress_config["target"]["options"]["sfSchema"]}.{raw_tbl}_staging"""
            merged_tbl = f"""{egress_config["target"]["options"]["dbtable"]}_merged"""
            sf_merged_tbl = f"""{egress_config["target"]["options"]["sfDatabase"]}.{egress_config["target"]["options"]["sfSchema_tgt"]}.{merged_tbl}"""
            stg_tbl = f"""{egress_config["target"]["options"]["dbtable"]}_staging"""
            sf_staging_tbl = f"""{egress_config["target"]["options"]["sfDatabase"]}.{egress_config["target"]["options"]["sfSchema"]}.{stg_tbl}"""

            if 'guidewire' in egress_config["data"]["eventTypeId"].lower():
                #raw DDL creation
                sf_write.sf_table_creation(raw_tbl,sf_raw_tbl,sfoptions1)

                #Checking for column addition from Schema Evolution and executing alter statement if new column is added-RAW and RAW STG
                
                sf_write.sf_schema_evolve(raw_tbl, sfoptions1, sf_raw_tbl)
                sf_write.sf_schema_evolve(raw_tbl, sfoptions2, sf_raw_stg_tbl)

                #Generating insert query for Raw target table to be executed in snowflake
                insert_query_raw_stg=f""" insert overwrite into {sf_raw_tbl} select distinct * from {sf_raw_stg_tbl};"""

                #writing the raw staging table
                mode = 'append'
                output_mode = 'append'
                dbtable = egress_config['target']['options']['dbtable']+'_raw_staging'
                checkpoint = egress_config['target']['options']['checkpointLocation']+'_raw'
                sf_write.snowflake_stream_write(final_raw_df,mode,output_mode,dbtable,egress_config,checkpoint,insert_query_raw_stg)

                if egress_config["target"]["mergeQueryFlag"] == 'Y':
                    #merged DDL creation
                    sf_write.sf_table_creation(merged_tbl, sf_merged_tbl, sfoptions1)

                    #Checking for column addition from Schema Evolution and executing alter statement if new column is added-MERGED
                    sf_write.sf_schema_evolve(merged_tbl, sfoptions1, sf_merged_tbl)

                    #Generating the merge query for merged table to be executed in snowflake
                    merge_query= sf_write.merge_query_generate(merged_tbl,sf_merged_tbl,sf_staging_tbl)

                    # Writing data into merged staging in snowflake and executing insert query for raw target and merge query for merged target table
                    mode = 'overwrite'
                    output_mode = 'update'
                    dbtable = egress_config['target']['options']['dbtable']+'_staging'
                    checkpoint = egress_config['target']['options']['checkpointLocation']+'_merged'
                    sf_write.snowflake_stream_write(final_merged_df,mode,output_mode,dbtable,egress_config,checkpoint,merge_query)
            else:
                #writing the data in snowflake
                mode = 'append'
                output_mode = 'append'
                dbtable = egress_config['target']['options']['dbtable']
                checkpoint = egress_config['target']['options']['checkpointLocation']
                query="No query"
                sf_write.snowflake_stream_write(final_df,mode,output_mode,dbtable,egress_config,checkpoint,query)

        if egress_config["target"]["targetType"] in ( 'postgresql', 'oracle', 'sqldb', 'jdbc', 'sqlserver', 'mysql'):
            jdbc_write.jdbc_write(final_df, egress_config)

        if egress_config["target"]["targetType"] == 'redshift':
            redshift_write.redshift_stream_write(final_df, egress_config)

        if egress_config["target"]["targetType"] == 'blob':
            blob_write.blob_data_write(final_df, egress_config)
        
        if egress_config["target"]["targetType"] == 's3':
            s3_write.s3_data_write(final_df, egress_config)

        if egress_config["target"]["targetType"] == 'api':
            api_write.api_write(final_df, egress_config)


    except Exception as e:
        import traceback
        print(traceback.format_exc())
        err_msg = f"Encountered exception during Persistance of the job config - {e}"
        print(err_msg)
        raise Exception(e)