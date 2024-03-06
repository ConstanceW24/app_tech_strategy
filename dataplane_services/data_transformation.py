import sys
from pyspark.sql import SparkSession, utils as spark_utils, DataFrame
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from datetime import datetime
from common_utils import msi_transformation as msi, utils as utils
from common_utils.batch_writer import batch_writer
from common_utils.custom_handler import *
from data_quality_validations import *
from dataplane_services.data_quality import *
from copy import deepcopy

spark= SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

def foreachbatch_transformation(i,source_file_name,src,ingress_config):

    def foreach_handler(batchdf, batchid):
        batchdf.createOrReplaceTempView(f'{source_file_name}_batch')
        if i['rule_parser'].lower() == 'sparksql':
            df = batchdf._jdf.sparkSession().sql(i['rule_override'].format(*list(map(eval,i['Transformation_input']))))
            df = DataFrame(df, spark)
        elif i['rule_parser'].lower() == 'custom_handler':
            df = eval(i['rule_override'])(batchdf,i['Transformation_input'])

        if ingress_config.get('dqrules', None)  is not None:
            df = foreachbatch_postdtchecks(df)

        batch_writer(df,ingress_config)

    def foreachbatch_postdtchecks(df):
        if ingress_config['dqrules'].get('postdtchecks','').lower() == 'true':
            dq_config = deepcopy(ingress_config)
            dq_config['rules'] = dq_config['dqrules']['rules']
            df = stream.batch_data_quality_check(df,dq_config)

            if ingress_config['target'].get('rejectOptions','') != '':
                df, rejected_df = stream.get_rejected_records(df,dq_config)
                reject_data_config, reject_summary_config = stream.generate_reject_configs(dq_config)
                batch_writer(rejected_df, reject_data_config)

                if rejected_df is not None:
                    summary_df = stream.generate_summary(rejected_df,dq_config)
                    batch_writer(summary_df, reject_summary_config)
        return df

    stream_df = src.writeStream.queryName(f"DataTransformation-{source_file_name}")\
                .foreachBatch(foreach_handler) \
                .outputMode("append")\
                .option("checkpointLocation", ingress_config["target"]["options"]["checkpointLocation"])\
                .trigger(once=True) \
                .start()

    utils.monitor_and_stop_stream(stream_df)

#parsing rules for non-upsert
def rule_transformation(rulesno,source_file_name,src):
    """
    to parse the rules mentioned in the config and perform transformation

    Parameters
    ----------
    ingress_config
    rulesno
    source_file_name
    src : return from source_df_read method

    Return
    ------
    dataframe(src) after applies the transformation rules

    """

    if(rulesno['Transformation_name'].lower() in ("select_transformation","join_transformation")) & (rulesno['rule_parser'].lower()=='sparksql'):
        print(rulesno['Transformation_name'])
        src.createOrReplaceTempView('src')
        src = spark.sql(rulesno['rule_override'].format(*list(map(eval,rulesno['Transformation_input']))))
        spark.catalog.dropTempView('src')

    if(rulesno['Transformation_name'].lower() in ("select_transformation")) & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src = src.selectExpr(*rulesno['Transformation_input'])
        src.createOrReplaceTempView('src')

    # Transformation_input should be an array of strings (column names)
    if(rulesno['Transformation_name'].lower()=="dropcolumns_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.drop(*rulesno['Transformation_input'])
        src.createOrReplaceTempView(source_file_name)

    # Transformation_input should be a string containing the filter condition
    if(rulesno['Transformation_name'].lower()=="filter_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.where(rulesno['Transformation_input'][0])
        src.createOrReplaceTempView(source_file_name)

    # Transformation_input should be a string containing the constant value
    if(rulesno['Transformation_name'].lower()=="defaultcolumn_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.withColumn(rulesno['Transformation_output'][0],F.lit(*rulesno['Transformation_input']))
        src.createOrReplaceTempView(source_file_name)

    if(rulesno['Transformation_name'].lower() in ("select_transformation")) & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src = src.select(*rulesno['Transformation_input'])
        src.createOrReplaceTempView(source_file_name)

        # Transformation_input should be a string containing the constant value
    if(rulesno['Transformation_name'].lower()=="expressioncolumn_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.withColumn(rulesno['Transformation_output'][0],F.expr(rulesno['Transformation_input'][0]))
        src.createOrReplaceTempView(source_file_name)

    # Transformation_input should be a string containing the constant value
    if(rulesno['Transformation_name'].lower()=="addcolumn_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.withColumn(rulesno['Transformation_output'][0],rulesno['Transformation_input'][0])
        src.createOrReplaceTempView(source_file_name)

    # Transformation_input should be a string containing the constant value
    if(rulesno['Transformation_name'].lower()=="renamecolumn_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.withColumnRenamed(rulesno['Transformation_input'][0],rulesno['Transformation_output'][0])
        src.createOrReplaceTempView(source_file_name)


    if(rulesno['Transformation_name'].lower()=="distinct_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.distinct()
        spark.catalog.dropTempView(source_file_name)
        src.createOrReplaceTempView(source_file_name)

    if rulesno['rule_parser'].lower() == 'custom_handler':
        print(rulesno['rule_parser']+'-'+rulesno['rule_override'])
        src = eval(rulesno['rule_override'])(src, rulesno['Transformation_input'])

    return src

def main_trans_select(ingress_config, source_file_name, process_name, src):

    sorted_rule_list = sorted(ingress_config['rules'], key=lambda d: d['rule_id'])

    for i in sorted_rule_list:
        if((i['Transformation_name'].lower()=="merge_transformation") & (i['rule_parser'].lower()=='sparksql')):
                        ########### Upsert To Delta Merge Operation method ############
            msi.upsert_merge_tranformation(source_file_name, src, ingress_config)
        elif((i['Transformation_name'].lower()=="foreachbatch_transformation") & (i['rule_parser'].lower() in ['sparksql','custom_handler'])):
            foreachbatch_transformation(i,source_file_name, src, ingress_config)
        else:
             src=rule_transformation(i, source_file_name, src)

    if not any(item.get('Transformation_name', '').lower() == 'foreachbatch_transformation' for item in ingress_config['rules']):
        if ingress_config.get('dqrules', None)  is not None:
            if ingress_config['dqrules'].get('postdtchecks','').lower() == 'true':
                dq_config = deepcopy(ingress_config)
                dq_config['rules'] = dq_config['dqrules']['rules']
                src=stream_data_quality_check(src,dq_config)

                ###########Write the Stream into S3 CSV ##########
        if src is not None:
            stream.file_storage_write_stream(process_name, src, ingress_config)

def on_failure_create_empty_df(i, ae):
    if 'Path does not exist' in str(ae) or 'FileNotFoundException' in str(ae):
        print(f'Dependant table not available: {ae}')
                            ## Create empty dependant table during full load for tables that have to merge with themselves
        print('Creating empty dataframe')
        emp_rdd = spark.sparkContext.emptyRDD()
        dependents_data = spark.createDataFrame(data = emp_rdd,
                                            schema = i["schema"])
        dependents_data.createOrReplaceTempView(i['filename'])
    else:
        raise ValueError(f"Exception is not: Path does not exist, Exception : {ae}")

def read_dependants(ingress_config):
    for i in ingress_config['source']['dependents']:
        if i['SourceType'] in ['blob','s3']:
            file_location = i['path'] + i['filename'] + '/'

            if i.get('partitionDefault', '') != "":
                partition_key           = i['partitionKey']
                partition_default_logic = i['partitionDefault']
                partition_value         = str(eval(partition_default_logic))
                file_location += f"{partition_key}={partition_value}/"

            dependents_data = (spark
                                .read
                                .format(i["inputFileFormat"])
                                .option("header", True))# To read the first row as header

            if i["schema"].upper() == 'NA':
                    ## Infer schema if not provided in config
                dependents_data = dependents_data.option("inferSchema", True)
            else:
                dependents_data = dependents_data.schema(i["schema"])

            try:
                dependents_data = dependents_data.load(file_location)
                dependents_data.createOrReplaceTempView(i['filename'])

            except Exception as ae:
                print(str(ae))
                on_failure_create_empty_df(i, ae)


def pre_dq_validations(src, ingress_config, source_file_name):

    if ingress_config.get('dqrules', None)  is not None:
        if str(ingress_config['dqrules'].get('postdtchecks','')).lower() in ['','false','none']:
            if ingress_config['dqrules'].get('rules', None) is not None:
                for i in ingress_config['dqrules']['rules']:

                    valid_check = f'''{i['validation_name']}(src,i)'''
                    src=eval(valid_check)
                    src.createOrReplaceTempView(source_file_name)
    return src

#primary transformation method
def data_transformation(ingress_config):
    """Gets the data transformation config and read the payload data from source location, performs Data transformations,
       reads the external data to enrich the payloa data and write the transformed payload data to target location for next service.
    Parameters
    ----------
    df_config : json
        The data transformation configuration

    Conditions
    ----------
    validated_config : bool
        A condition to display the status of config validation returned from comutil.parse_and_validate_config().
    df_config["source"]["driver"]["SourceType"] : string
        A condition to check the SourceType and invoke the respective function from S3Stream/KafkaStream class files to read the data event.
    df_config["target"]["targetType"] : string
        A condition to check the targetType and invoke the respective function from S3Stream/KafkaStream class files to write the data event.
    Transformation_name: string
        This Script reads the Spark sql query from rule_override from the configuration and performs the below transformations.
            1. Field Level Transformation : This transformation uses the in-built functions like Cancat,length,convert..etc to perform field level transformation.
            2. Select : This transformation limits the no. of columns from the streaming/dependent files.
            3. Where : This transformation filters the data from streaming/dependent files.
            4. Distinct : This transformation eliminates the redundant data from the streaming/dependent files.
            5. Drop : This transformation drops the columns from the final dataframe before persistent.
            6. Default : This transformation adds the new column with static value
            7. Streaming data will join with other dependent data and fetch the desired result.
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
    process_name        = f"DataTransformation-{source_file_name}"


    srcdf = stream.source_df_readstream(ingress_config)
    srcdf.createOrReplaceTempView(source_file_name)

    ############## Data Quality Validations ##################
    src = pre_dq_validations(srcdf,ingress_config, source_file_name)

    ############## Reading the Dependent data ##################
    read_dependants(ingress_config)

    ############## Iterating the Data Transformation Rules ##################

    try:
        rule_msi_check = 0
        if (ingress_config['rules'] != []):
            if ((ingress_config['rules'][0]['Transformation_name'].lower()=="json_denormalizer") & (ingress_config['rules'][0]['rule_parser'].lower() in ['pyspark'])):
                rule_msi_check = 1

        if (rule_msi_check==1):
            msi.main_json_flatten(src, ingress_config, ingress_config['rules'][0])
        else:
            main_trans_select(ingress_config, source_file_name, process_name, src)

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        err_msg = f"Encountered exception during Transformation of the job config - {e}"
        print(err_msg)
        raise Exception(e)