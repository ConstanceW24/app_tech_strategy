from common_utils import  utils as utils
from ingress_utils import blob_read, confluent_kafka_read, jdbc_read, s3_read, msk_read, eventhub_read, cobol_read
from egress_utils import blob_write, s3_write
from pyspark.sql import SparkSession, DataFrame
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



def gw_cast_datatype(source_df):
    from pyspark.sql.functions import col
    for field in source_df.schema.fields:
        if 'gwcbi___' in field.name:
            source_df=source_df.withColumn(field.name,col(field.name).cast("string"))
    return source_df


def cdc_write_stream(stream_df, ingress_config):

    def cdc_foreach_writer(microBatchOutputDF, batchId):
        
        microBatchOutputDF.write.mode("append").format("delta").partitionBy("date_partition").saveAsTable(cdc_lnd_table)

    date_partition=datetime.today().strftime('%Y%m%d%H%M')
    cdc_lnd_table=f'''{ingress_config["data"]["inputFile"]["fileName"]}_lnd'''
    stream_df=stream_df.withColumn("date_partition",lit(date_partition))

    if ingress_config["target"]["noofpartition"]:
        stream_df = stream_df.coalesce(int(ingress_config["target"]["noofpartition"])).writeStream
    else:
        stream_df = stream_df.writeStream

    kwargs=ingress_config["target"]["options"]
    if len(kwargs) > 0:
        for k,v in kwargs.items():
            if k == "format":
                stream_df = stream_df.format(v)
            else:
                stream_df = stream_df.option(k,v)
            if ingress_config["target"]["targetTrigger"] !='once':
                stream_df=stream_df.trigger(processingTime=ingress_config["target"]["targetTrigger"])
            else:
                stream_df=stream_df.trigger(once=True)
            stream_df=stream_df.outputMode("append")

    stream_df.foreachBatch(cdc_foreach_writer).start()

    

def source_df_readstream(ingress_config):
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
    elif ingress_config["source"]["driver"]["SourceType"] == 'msk':
            raw_df = msk_read.read_mskstream(ingress_config)
            raw_df=kafka_deserialization(raw_df) 
    elif ingress_config["source"]["driver"]["SourceType"] == 'eventhub':
            raw_df = eventhub_read.read_eventhub(ingress_config)
            print("eventhub source read")
            raw_df=kafka_deserialization(raw_df) 
    elif ingress_config["source"]["driver"]["SourceType"] == 'msi_msk':
        from pyspark.sql.avro.functions import from_avro
        raw_df,schemastr,schemasubject = msk_read.read_msk_kafkastream(ingress_config)
        raw_df = raw_df.select(from_avro(col("value"), schemasubject,schemastr).alias("value"))
        schema = ingress_config["data"]["inputFile"]["schema"]
        raw_df = raw_df.select(col(schema))

    elif ingress_config["source"]["driver"]["SourceType"] in ['s3']:
        raw_df = s3_read.s3_data_read(ingress_config)
    elif ingress_config["source"]["driver"]["SourceType"] in ['blob']:
        raw_df = blob_read.blob_data_read(ingress_config)
    # else: #ingress_config["source"]["driver"]["SourceType"] == 'sqldb':
    #     raw_df=jdbc_read.sqldb_stream_read(ingress_config)

    ## Adding xml flatten logic when explodColumn parameter is provided
    if "xml" in  ingress_config["source"]["driver"]["format"].lower() and ingress_config["source"]["driver"].get("explodeColumn","") != "":
        exp_col = ingress_config["source"]["driver"]["explodeColumn"]
        raw_df = utils.df_flatten_attr(raw_df, exp_col=exp_col)

    ## handling GW cda column datatype mistmatch
    if 'guidewire' in ingress_config["data"]["eventTypeId"].lower():
        raw_df = gw_cast_datatype(raw_df)


    ## Adding fingerprint & timestamp information into dataframe
    if ingress_config["miscellaneous"]:
        for i in range(0,len(ingress_config["miscellaneous"]),2):
            raw_df = raw_df.withColumn(ingress_config["miscellaneous"][i], split(input_file_name(),'/').getItem(int(ingress_config["miscellaneous"][i+1])))


    ## Drop Hudi Metadata columns '_hoodie'
    if ingress_config["data"]["inputFile"]["inputFileFormat"] in ["org.apache.hudi","hudi"]:
        raw_df = utils.drop_columns(raw_df, '_hoodie')

    ## Add additional fields
    if ingress_config.get("addColumns",None) != None:
        for cols in ingress_config["addColumns"].keys():
            raw_df = raw_df.withColumn(cols, eval(ingress_config["addColumns"][cols]))

    return raw_df



def file_storage_write_stream(process_name, raw_df, egress_config):
    """
    to write the data into the sink based on configuration input using streams into s3/Blob

    Parameters
    ----------
    egress_config
    src : return from source_df_read method
    source_file_name

    Output
    ------
    Writes the data into specfified sink

    """
    processing_engine = egress_config.get("processingEngine", "")
    table_name = process_name.split('-')[1]
    print(processing_engine,table_name)
    # Fixing the header
    if str(egress_config["target"].get("fixHeader", None)).lower() == "true":
        raw_df = utils.fix_headers(raw_df)

    #if 'cdc' in str(egress_config.get('configId', None)).lower():
    if 'cdc' in egress_config["data"]["eventTypeId"].lower():
        cdc_write_stream(raw_df,egress_config)
    # Selecting proper writer
    elif egress_config["target"]["targetType"] == 'blob':
        blob_write.blob_data_write(raw_df, egress_config,process_name)
    elif egress_config["target"]["targetType"] == 's3' :
        s3_write.s3_data_write(raw_df, egress_config,process_name)



def foreachbatch_dq_checks(rule_list, src ,ingress_config, summarize = False):

    source_file_name = ingress_config["data"]["inputFile"]["fileName"].replace("/","")

    def foreach_handler(batchdf, batchid):
        batchdf = batch_data_quality_check(batchdf,ingress_config, rule_list)

        if ingress_config['target'].get('rejectOptions','') != '':
            batchdf, rejected_df = get_rejected_records(batchdf,ingress_config)
            reject_data_config, reject_summary_config = generate_reject_configs(ingress_config)
            batch_writer(rejected_df, reject_data_config, spark)

            if rejected_df is not None:
                summary_df = generate_summary(rejected_df,ingress_config)
                batch_writer(summary_df, reject_summary_config, spark)

        batch_writer(batchdf, ingress_config, spark)

    def write_summary(batchdf, _):
        summary_df = generate_summary(batchdf,ingress_config)
        batch_writer(summary_df,ingress_config, spark)

    handler = write_summary if summarize else foreach_handler

    print(handler)

    stream_df = src.writeStream.queryName(f"{source_file_name}_foreach")\
                .foreachBatch(handler) \
                .outputMode("append")\
                .option("checkpointLocation", ingress_config["target"]["options"]["checkpointLocation"])\
                .trigger(once=True) \
                .start()

    utils.monitor_and_stop_stream(stream_df)

def batch_data_quality_check(batchdf, ingress_config, rule_list=None):
    print("Running data quality checks in batch")
    if rule_list is None:
        rule_list = ingress_config['rules']
    if ingress_config['target'].get('rejectOptions','') != '':
        for i in rule_list:
            i['reject_handling'] = 'false'
    for i in rule_list:
        print(f'Performing {i} dq check')
        valid_check = f'''{i['validation_name']}(batchdf, i)'''
        batchdf = eval(valid_check)
    return batchdf

def generate_summary(batchdf,ingress_config):
    print("Generating summary df")
    reject_columns = get_reject_columns(ingress_config)
    select_list = get_summary_select_list(ingress_config)
    batchdf.createOrReplaceTempView('rejected_data')
    df = batchdf._jdf.sparkSession().sql(f"""
        select 
        Table, 
        AppId,
        Runtime, 
        stack({len(reject_columns)}, {','.join(["'"+r+"'," + r for r in reject_columns])}) as (ValidationName, RejectedCount) 
        from (
            select 
            '{ingress_config["tableName"]}' as `Table`, 
            '{ingress_config["appId"]}' as `AppId`, 
            '{ingress_config["runTime"]}' as `Runtime`, 
            {select_list} 
            from rejected_data )
    """)
    df = DataFrame(df,spark)
    return df


def generate_reject_configs(ingress_config):
    curr_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    reject_config = deepcopy(ingress_config)
    reject_config['target']['options'] = reject_config['target']['rejectOptions']
    reject_options = {"header" : "true", "format" : "csv"}
    reject_config['target']['options'] = {**reject_config['target']['options'], **reject_options}
    reject_config['target']['noofpartition'] = 1

    reject_data_config = deepcopy(reject_config)
    reject_data_config['target']['options']['path'] = f"{reject_data_config['target']['options']['path']}/data/{curr_timestamp}"
    reject_data_config['target']['options']['checkpointLocation'] = f"{reject_data_config['target']['options']['checkpointLocation']}/data/{curr_timestamp}"

    reject_summary_config = deepcopy(reject_config)
    if not reject_summary_config['target']['options']['path'].endswith('/'):
        reject_summary_config['target']['options']['path'] = reject_summary_config['target']['options']['path'] + '/'
    reject_summary_config['target']['options']['path'] = f"{dirname(dirname(reject_summary_config['target']['options']['path']))}/summary/"
    print("reject_summary_path: ",  reject_summary_config['target']['options']['path'])
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

def get_summary_select_list(ingress_config):
    reject_columns = get_reject_columns(ingress_config)
    select_list = ','.join([f"SUM(CASE WHEN {x} = 'Fail' THEN 1 ELSE 0 END) as {x}" for x in reject_columns])
    return select_list
