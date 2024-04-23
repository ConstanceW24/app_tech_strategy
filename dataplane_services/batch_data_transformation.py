import sys
from pyspark.sql import SparkSession, utils as spark_utils, DataFrame
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
from common_utils import utils as utils,db_restapi_methods as methods, batch_process_utils as batch
from common_utils.custom_handler import *
from data_quality_validations import *
from dataplane_services.batch_data_quality import *
from copy import deepcopy

spark= SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

dbutils = methods.get_dbutils(spark)


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
    if(rulesno['Transformation_name'].lower() in ("select_transformation","join_transformation")) & (rulesno['rule_parser'].lower()=='sqlfile'):
        print(rulesno['Transformation_name'])
        src.createOrReplaceTempView(source_file_name)
        dbfs=rulesno['rule_override']
        file_content=dbutils.fs.head(dbfs)
        src = spark.sql(f'{file_content}'.format(*list(map(eval,rulesno['Transformation_input']))))

    if(rulesno['Transformation_name'].lower() in ("select_transformation","join_transformation")) & (rulesno['rule_parser'].lower()=='sparksql'):
        print(rulesno['Transformation_name'])
        src.createOrReplaceTempView(source_file_name)
        src = spark.sql(rulesno['rule_override'].format(*list(map(eval,rulesno['Transformation_input']))))

    if(rulesno['Transformation_name'].lower() in ("select_transformation")) & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src = src.selectExpr(*rulesno['Transformation_input'])

    # Transformation_input should be an array of strings (column names)
    if(rulesno['Transformation_name'].lower()=="dropcolumns_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.drop(*rulesno['Transformation_input'])

    # Transformation_input should be an array of strings (column names)
    if(rulesno['Transformation_name'].lower()=="patterndropcolumns_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        import re
        src=src.drop(*[cl for cl in src.columns if re.match(rulesno['Transformation_input'][0],cl)])

    # Transformation_input should be a string containing the filter condition
    if(rulesno['Transformation_name'].lower()=="filter_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.where(rulesno['Transformation_input'][0])

    # Transformation_input should be a string containing the constant value
    if(rulesno['Transformation_name'].lower()=="keygen_transformation"):
        print(rulesno['Transformation_name'])
        src = key_generator(src, rulesno['Transformation_output'])

    # Transformation_input should be a string containing the constant value
    if(rulesno['Transformation_name'].lower()=="defaultcolumn_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.withColumn(rulesno['Transformation_output'][0],F.lit(*rulesno['Transformation_input']))

    # Transformation_input should be a string containing the constant value
    if(rulesno['Transformation_name'].lower()=="expressioncolumn_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.withColumn(rulesno['Transformation_output'][0],F.expr(rulesno['Transformation_input'][0]))

    # Transformation_input should be a string containing the constant value
    if(rulesno['Transformation_name'].lower()=="addcolumn_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.withColumn(rulesno['Transformation_output'][0],rulesno['Transformation_input'][0])

    # Transformation_input should be a string containing the constant value
    if(rulesno['Transformation_name'].lower()=="renamecolumn_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.withColumnRenamed(rulesno['Transformation_input'][0],rulesno['Transformation_output'][0])

    if(rulesno['Transformation_name'].lower()=="distinct_transformation") & (rulesno['rule_parser'].lower()=='pyspark'):
        print(rulesno['Transformation_name'])
        src=src.distinct()
        #spark.catalog.dropTempView(source_file_name)

    if(rulesno['Transformation_name'].lower()=="scd1_transformation"):
        print(rulesno['Transformation_name'])
        src = scd1_transformation(src, rulesno['Transformation_input'])

    if(rulesno['Transformation_name'].lower()=="scd2_transformation"):
        print(rulesno['Transformation_name'])
        src = scd2_transformation(src, rulesno['Transformation_input'])
        

    if rulesno['rule_parser'].lower() == 'custom_handler':
        print(rulesno['rule_parser']+'-'+rulesno['rule_override'])
        src = eval(rulesno['rule_override'])(src, rulesno['Transformation_input'])

    src.createOrReplaceTempView(source_file_name)
    return src

def run_predqchecks(src, ingress_config,  source_file_name):

    if ingress_config.get('dqrules', None)  is not None:
        if str(ingress_config['dqrules'].get('postdtchecks','')).lower() in ['','false','none']:
            if ingress_config['dqrules'].get('rules', None) is not None:
                for i in ingress_config['dqrules']['rules']:

                    valid_check = f'''{i['validation_name']}(src,i)'''
                    src=eval(valid_check)
                    src.createOrReplaceTempView(source_file_name)
    return src


def handle_empty_dependent(ae,i, file_location):
    if 'Path does not exist' in str(ae) or 'FileNotFoundException' in str(ae):
        print(f'Dependant table not available: {ae}')
        ## Create empty dependant table during full load for tables that have to merge with themselves
        print('Creating empty dataframe')
        emp_rdd = spark.sparkContext.emptyRDD()
        if i["schema"] != 'NA' :
            dependents_data = spark.createDataFrame(data = emp_rdd,
                            schema = i["schema"])

            if i.get("saveIfEmpty","").lower() == "true":
                if i["inputFileFormat"] == 'delta':
                    dependents_data.write.format(i["inputFileFormat"]).option("path",file_location).saveAsTable(i["deltaTable"])
                else:
                    dependents_data.write.format(i["inputFileFormat"]).save(file_location)
                dependents_data = spark.read.format(i["inputFileFormat"]).load(file_location)

            dependents_data.createOrReplaceTempView(i['filename'])
    else:
        raise ValueError(f"Exception : {ae}")

def load_dependents(ingress_config):
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
                handle_empty_dependent(ae,i, file_location)


#primary transformation method
def batch_data_transformation(ingress_config):
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


    src = batch.source_df_read(ingress_config, process_name)
    src.createOrReplaceTempView(source_file_name)

    ############## Data Quality Validations ##################
    src = run_predqchecks( src, ingress_config, source_file_name)

    ############## Reading the Dependent data ##################

    load_dependents(ingress_config)

    ############## Iterating the Data Transformation Rules ##################

    try:
        sorted_rule_list = sorted(ingress_config['rules'], key=lambda d: d['rule_id'])

        for i in sorted_rule_list:
            src = rule_transformation(i, source_file_name, src)


        if ingress_config.get('dqrules', None)  is not None:
            if ingress_config['dqrules'].get('postdtchecks','').lower() == 'true':
                dq_config = deepcopy(ingress_config)
                dq_config['rules'] = dq_config['dqrules']['rules']
                src = data_quality_check(src,dq_config)

        ###########Write the Stream into S3 CSV ##########
        if src is not None:
            batch.file_storage_write(process_name, src, ingress_config)


    except Exception as e:
        import traceback
        print(traceback.format_exc())
        err_msg = f"Encountered exception during Transformation of the job config - {e}"
        print(err_msg)
        raise Exception(e)



def task_batch_data_transformation(src,config):


    #Fetching source file details and creating view for future use
    source_file_name    = config["sourceTableAlias"].replace("/","")
    process_name        = f"DataTransformation-{source_file_name}"

    src.createOrReplaceTempView(source_file_name)

    ############## Data Quality Validations ##################
    src = run_predqchecks( src, config, source_file_name)

    ############## Reading the Dependent data ##################

    #load_dependents(config)

    ############## Iterating the Data Transformation Rules ##################

    try:

        for i in config['rules']:
            src = rule_transformation(i, source_file_name, src)


        if config.get('dqrules', None)  is not None:
            if config['dqrules'].get('postdtchecks','').lower() == 'true':
                dq_config = deepcopy(config)
                dq_config['rules'] = dq_config['dqrules']['rules']
                src = data_quality_check(src,dq_config)

        ###########Write the Stream into S3 CSV ##########
        if src is not None:
            return src


    except Exception as e:
        import traceback
        print(traceback.format_exc())
        err_msg = f"Encountered exception during Transformation of the job config - {e}"
        print(err_msg)
        raise Exception(e)