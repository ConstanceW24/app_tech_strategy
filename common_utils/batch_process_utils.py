from common_utils import utils as utils
from ingress_utils import blob_read, confluent_kafka_read, jdbc_read, s3_read, msk_read, eventhub_read, cobol_read
from egress_utils import blob_write, jdbc_write, s3_write, api_write, delta_write, kafka_write, eventhub_write, msk_write
from egress_utils import snowflake_write as sf_write, redshift_write
from pyspark.sql import SparkSession
from pyspark.sql.types  import *
from pyspark.sql.functions import (array, coalesce, col, count, current_date,
                                   datediff, explode, expr, length, lit, lower,
                                   regexp_replace, split, substring, sum,
                                   to_date, to_timestamp, trim, udf, when,
                                   from_json,input_file_name)
from common_utils.batch_writer import batch_writer
import datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F
from copy import deepcopy
from os.path import dirname

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")


def source_df_read(ingress_config, process_name):
    """
    to obtain the read stream df based on the input source  specified in configuration s3,Blob,Kafka

    Parameters
    ----------
    ingress_config

    Return
    ------
    Provides the readstream dataframe

    """

    def kafka_deserialization(raw_df):
        schemastr = ingress_config["data"]["inputFile"]["schema"]
        raw_df = raw_df.selectExpr("CAST(value AS STRING)") \
                        .select(from_json(col("value"), schemastr).alias("json")) \
                        .select("json.*")
        return raw_df

    if ingress_config["source"]["driver"]["SourceType"] == 'kafka':
        raw_df = confluent_kafka_read.read_kafkastream(ingress_config) 
        raw_df=kafka_deserialization(raw_df)

    elif ingress_config["source"]["driver"]["SourceType"] == 'eventhub':
        raw_df = eventhub_read.read_eventhub(ingress_config)
        raw_df=kafka_deserialization(raw_df)

    elif "cobol" in  ingress_config["source"]["driver"]["format"].lower():
        raw_df = cobol_read.cobol_read(ingress_config)

    elif "jdbc" in  ingress_config["source"]["driver"]["format"].lower():
        raw_df = jdbc_read.db_read(ingress_config)

    elif ingress_config["source"]["driver"]["SourceType"] in ['s3']:
        raw_df = s3_read.s3_data_read(ingress_config)

    elif ingress_config["source"]["driver"]["SourceType"] in ['blob']:
        raw_df = blob_read.blob_data_read(ingress_config)
      
    ## Adding xml flatten logic when explodColumn parameter is provided
    if "xml" in  ingress_config["source"]["driver"]["format"].lower() and ingress_config["source"]["driver"].get("explodeColumn","") != "":
        exp_col = ingress_config["source"]["driver"]["explodeColumn"]
        raw_df = utils.df_flatten_attr(raw_df, exp_col=exp_col)

    ## Adding fingerprint & timestamp information into dataframe
    if ingress_config.get("miscellaneous", []) != []:
        for i in range(0,len(ingress_config["miscellaneous"]),2):
            raw_df = raw_df.withColumn(ingress_config["miscellaneous"][i], split(input_file_name(),'/').getItem(int(ingress_config["miscellaneous"][i+1])))


    ## Drop Hudi Metadata columns '_hoodie'
    if ingress_config["data"]["inputFile"]["inputFileFormat"] in ["org.apache.hudi","hudi"]:
        raw_df = utils.drop_columns(raw_df, '_hoodie')

    ## Add additional fields
    if ingress_config.get("addColumns",None) != None:
        for cols in ingress_config["addColumns"].keys():
            raw_df = raw_df.withColumn(cols.upper(), eval(ingress_config["addColumns"][cols]))

    reconcile_write(process_name, raw_df, ingress_config)

    return raw_df



def reconcile_write(process_name, raw_df, ingress_config):
    if ingress_config.get("reconcilePath","") != "":
        runtime = ingress_config["runTime"]
        table_name = ingress_config["tableName"]
        node_processes = ingress_config["nodeProcess"]
        app_id = ingress_config["appId"]
        run_id = ingress_config['payload_id'][0]
        reject_count = 0
        df_count = raw_df.count()
        if process_name.endswith("_rejected"):
            reject_count = df_count
            df_count = 0
        reconcile = [(run_id, app_id, runtime, table_name, node_processes, df_count, 0, reject_count)]
        df = spark.createDataFrame(reconcile,["RunId", "AppId", "Runtime", "Table", "Stage", "SourceCount", "TargetCount", "RejectedCount"])
        write_df_to_path(df, ingress_config["reconcilePath"])


def count_validation_check(ingress_config):
    if ingress_config.get("reconcilePath","") != "":
        from pyspark.sql.functions import max, col
        df = spark.read.csv(ingress_config.get("reconcilePath",""), header="true").where(f"""AppId like '{ingress_config["appId"]}'""")\
        .groupBy("RunId","AppId")\
        .agg(max(col("Table")).alias("table"),
             max(col("Stage")).alias("stage"),
             sum(col("SourceCount").cast("int")).alias("source_count"),
             sum(col("TargetCount").cast("int")).alias("target_count"),
             sum(col("RejectedCount").cast("int")).alias("reject_count")
             )
        
        df.show()
        
        if df.where("source_count - reject_count = target_count").count() == 0 :
            raise Exception("Source Count - Reject count != Target Count ")
        else:
            print(df.where("source_count - reject_count = target_count").count())
            print("Record Count Matched !!")


def file_storage_write(process_name, raw_df, ingress_config):
    """
    to write the data into the sink based on configuration input using streams into s3/Blob

    Parameters
    ----------
    ingress_config
    src : return from source_df_read method
    source_file_name

    Output
    ------
    Writes the data into specfified sink

    """
    if process_name.endswith("_rejected"):
        reconcile_write(process_name, raw_df, ingress_config)

    # Fixing the header
    if str(ingress_config["target"].get("fixHeader", None)).lower() == "true":
        raw_df = utils.fix_headers(raw_df)

    # Selecting proper writer
    if ingress_config["target"]["targetType"] == 'blob':
        blob_write.blob_data_write(raw_df, ingress_config, process_name)

    elif ingress_config["target"]["targetType"] == 's3' :
        s3_write.s3_data_write(raw_df, ingress_config, process_name)

    elif ingress_config["target"]["targetType"] in ('postgresql', 'oracle', 'sqldb', 'jdbc', 'sqlserver', 'mysql'):
        jdbc_write.jdbc_write(raw_df, ingress_config, process_name)
    
    elif ingress_config["target"]["targetType"] == 'api':
        api_write.api_write(raw_df, ingress_config, process_name)

    else:
        raise Exception(f"Unable to handle {ingress_config['target']['targetType']}")




def write_summary(batchdf, ingress_config):
    summary_df = generate_summary(batchdf,ingress_config)
    batch_writer(summary_df,ingress_config, spark)

def generate_summary(batchdf, ingress_config):
    print("Generating summary df")
    reject_columns = get_reject_columns(ingress_config)
    select_list = get_summary_select_list(ingress_config)
    batchdf.createOrReplaceTempView('rejected_data')
    df = spark.sql(f"""
        select 
        Table,
        RunId, 
        AppId,
        Runtime, 
        stack({len(reject_columns)}, {','.join(["'"+r+"'," + r for r in reject_columns])}) as (ValidationName, RejectedCount) 
        from (
            select 
            '{ingress_config["tableName"]}' as `Table`, 
            '{ingress_config['payload_id'][0]}' as `RunId`, 
            '{ingress_config["appId"]}' as `AppId`, 
            '{ingress_config["runTime"]}' as `Runtime`, 
            {select_list} 
            from rejected_data )a
    """)
    return df

def get_summary_select_list(ingress_config):
    reject_columns = get_reject_columns(ingress_config)
    select_list = ','.join([f"SUM(CASE WHEN {x} = 'Fail' THEN 1 ELSE 0 END) as {x}" for x in reject_columns])
    return select_list

def format_path(path):
    if not path.strip().endswith('/'):
        return path.strip() + '/'
    else:
        return path


def generate_reject_configs(ingress_config):
    curr_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    reject_config = deepcopy(ingress_config)
    reject_config['target']['options'] = reject_config['target']['rejectOptions']
    if reject_config['target']['rejectOptions'].get('format','') == '':
        reject_options = {"header" : "true", "format" : "csv"}
    else:
        reject_options = {}
    reject_config['target']['options'] = {**reject_config['target']['options'], **reject_options}
    reject_config['target']['noofpartition'] = 1

    reject_data_config = deepcopy(reject_config)
    reject_data_config['target']['options']['path'] = f"{format_path(reject_data_config['target']['options']['path'])}data/appid={ingress_config['appId']}/{curr_timestamp}"
    reject_data_config['target']['options']['checkpointLocation'] = f"{reject_data_config['target']['options']['checkpointLocation']}/data/{curr_timestamp}"
    print("reject_data_path: ",  reject_data_config['target']['options'])

    reject_summary_config = deepcopy(reject_config)
    reject_options = {"header" : "true", "format" : "csv"}
    reject_summary_config['target']['options'] = {**reject_config['target']['options'], **reject_options}
    reject_summary_config['target']['options']['path'] = f"{dirname(dirname(reject_summary_config['target']['options']['path']))}/summary/appid={ingress_config['appId']}/{curr_timestamp}"
    print("reject_summary_path: ",  reject_summary_config['target']['options'])
    reject_summary_config['target']['options']['checkpointLocation'] = f"{reject_summary_config['target']['options']['checkpointLocation']}/summary/{curr_timestamp}"

    return reject_data_config, reject_summary_config


def get_reject_columns(ingress_config):
    reject_columns = []
    for i in ingress_config['rules']:
        if i['exception_handling'].lower() == 'reject':
            reject_columns.append(i['validation_output_field'])

    return reject_columns

def get_rejected_records(df,ingress_config):
    reject_columns = get_reject_columns(ingress_config)
    print(f"Creating rejected records for columns: {reject_columns}")

    if len(reject_columns) > 0:
        reject_exp =' | '.join([f'(col("{x}")!="Pass")' for x in reject_columns])
        rejected_df = df.filter(eval(reject_exp))
        # rejected_df.createOrReplaceTempView('rejected_data')
        passed_df = df.filter(~(eval(reject_exp)))

        return passed_df, rejected_df
    else:
        return df, None

def get_target_counts(ingress_config):
    try:
        count = spark.read.format(ingress_config["target"]["options"]["format"]).load(ingress_config["target"]["options"]["path"]).count()
        return count
    except Exception as e:
        if 'Path does not exist' in str(e) or 'FileNotFoundException' in str(e):
            return 0
        else:
            print("Exception in get_target_counts: ", str(e))

def write_df_to_path(df, path, format='csv', mode='append'):
    df = df.write.format(format).mode(mode)
    if format == 'csv':
        df = df.option('header','true')
    df.save(path)



def check_truncate_load(proc_config, project_config):
    from configs.app_config import set_client_functions
    client, processing_engine, get_content, get_list, clear_files, move_files, _, _ = set_client_functions(project_config)

    if proc_config['configuration']['target'].get('truncateLoad', '').lower() == "true" and proc_config['configuration']['target']['options'].get('path','') != '':
        try:
            clear_files(client, proc_config['configuration']['target']['options']['path'])
        except:
            pass


def get_old_target_counts(proc_config, reconcile_path):
    if reconcile_path is not None and reconcile_path != "":
        proc_config['configuration']['reconcilePath'] = reconcile_path
        
    if proc_config['configuration']['target'].get('options',{}).get('mode','append') != 'overwrite' and  proc_config['configuration']['target'].get('truncateLoad', 'false') != 'true':
        old_target_count = get_target_counts(proc_config['configuration'])
        print("old_target_count: ", str(old_target_count))
        return old_target_count
    else:
        return 0
    
def write_reconcile_count(proc_config, reconcile_path, old_target_count):
    if reconcile_path is not None and reconcile_path != "" :
        table = proc_config['configuration']['tableName']
        runtime = proc_config['configuration']['runTime']
        run_id = proc_config['configuration']['payload_id'][0]
        app_id = proc_config['configuration']['appId']
        stage = proc_config['configuration']['nodeProcess']
        new_target_count = get_target_counts(proc_config['configuration'])
        print("new_target_count: ", str(new_target_count))
        reconcile = [(run_id, app_id, runtime, table, stage, 0, new_target_count - old_target_count, 0)]
        df = spark.createDataFrame(reconcile,["RunId", "AppId", "Runtime", "Table", "Stage", "SourceCount", "TargetCount", "RejectedCount"])
        write_df_to_path(df, reconcile_path)
    else:
        print("Warning ! - Recon Path is not defined.")



def archive_target(proc_config, project_config):
    from configs.app_config import set_client_functions
    client, processing_engine, get_content, get_list, clear_files, move_files, _, _ = set_client_functions(project_config)
    if proc_config['configuration']['target'].get('archivePath','') != '':
        move_files(client, proc_config['configuration']['source']['driver']['path'], proc_config['configuration']['target']['archivePath'])

def clear_spark_metadata(proc_config, project_config):
    from configs.app_config import set_client_functions
    client, processing_engine, get_content, get_list, clear_files, move_files, _, _ = set_client_functions(project_config)
    if proc_config['configuration']['target'].get('options', {}) != {} and proc_config['configuration']['target']['options'].get('path', '') != '':
        tgt_path = proc_config['configuration']['target']['options']['path']
        if not tgt_path.endswith('/'):
            tgt_path += '/'
        try:
            clear_files(client, tgt_path + '_spark_metadata/')
        except Exception as e:
            if  not ('Path does not exist' in str(e) or 'FileNotFoundException' in str(e)):
                print(str(e))


def generate_validate_counts(proc_config):
    raw_count = 0
    trans_count = 0
    cleansed_count = 0
    final_count = 0

    if proc_config['configuration']["target"].get('validationKeys','') != '':
        key_cols = ','.join(proc_config['configuration']['target']['validationKeys'])
        count_col = 'count(distinct '+key_cols+')'
    else:
        count_col = 'count(*)'
    df = spark.sql(f"""
        select  load_date, 
                '{proc_config['configuration']['target']['options']['table']}' as table_name,
                {count_col} as row_count
        from {proc_config['configuration']['target']['options']['table']}
        group by load_date
        order by load_date desc
        limit 1 
    """)

    if df.count() != 0: 
        if proc_config['configuration']['target']['options']['table'].endswith('raw'):
            raw_count = df.first().row_count
        elif proc_config['configuration']['target']['options']['table'].endswith('trans'):
            trans_count = df.first().row_count
        elif proc_config['configuration']['target']['options']['table'].endswith('cleansed'):
            cleansed_count = df.first().row_count
        else: 
            final_count = df.first().row_count

    return raw_count, trans_count, cleansed_count, final_count


def write_validate_count(proc_config, validate_path):
    if validate_path is not None and validate_path != "" :
        table = proc_config['configuration']['tableName']
        runtime = proc_config['configuration']['runTime']
        run_id = proc_config['configuration']['payload_id'][0]
        app_id = proc_config['configuration']['appId']
        validate_type = "TargetTableValidateCount"
        raw_count, trans_count, cleansed_count, final_count = generate_validate_counts(proc_config) 
        reconcile = [(run_id, app_id, runtime, table, validate_type, raw_count, trans_count, cleansed_count, final_count)]
        df = spark.createDataFrame(reconcile,["RunId", "AppId", "Runtime", "Table", "ValidateType","RawCount", "TransCount", "CleansedCount", "FinalCount"])
        write_df_to_path(df, validate_path)
    else:
        print("Warning ! - Recon Path is not defined.")


def get_validate_summary(validate_path, run_id):
        from pyspark.sql.functions import max, col
        df = spark.read.csv(validate_path, header="true").where(f"""RunId like '{run_id}'""")\
        .groupBy("RunId")\
        .agg(max(col("Table")).alias("table"),
             sum(col("RawCount").cast("int")).alias("raw_count"),
             sum(col("TransCount").cast("int")).alias("trans_count"),
             sum(col("CleansedCount").cast("int")).alias("cleansed_count"),
             sum(col("FinalCount").cast("int")).alias("final_count")
             )
        
        df.show()




