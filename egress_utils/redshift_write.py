import datetime
from pyspark.sql import SparkSession
import boto3
import json
import sys
import os
from secrets_utils import secrets_manager
import traceback
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
from pyspark.sql.functions import col, to_json, lit, struct, concat, collect_list, concat_ws

spark = SparkSession.builder.getOrCreate()

val = "DECIMAL(38,8)"

def parse_credentials(egress_config):
    #Reading the Data Persistance Config Configurations
    if egress_config["target"]["options"].get('credentialSecret','') != "":
        secret_name = egress_config["target"]["options"].get('credentialSecret','')
    else:
        secret_name = "arn:aws:secretsmanager:us-east-1:442503347051:secret:Redshift_Credentials-lI9HOp"

    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client( service_name = 'secretsmanager',  region_name = region_name)
    get_secret_value_response = client.get_secret_value(SecretId = secret_name)

    secret = json.loads(get_secret_value_response['SecretString'])
    username=secret['username']
    password=secret['password']
    host=secret['host']
    port=secret['port']
    jdbcurl="jdbc:redshift://{0}:{1}/{2}".format(host, port, egress_config["target"]["options"].get("database",'act_int_dev'))

    credential = {
        "url": jdbcurl,
        "user": username,
        "password": password,
        "tempdir": egress_config["target"]["options"]["tempdir"],
        "forward_spark_s3_credentials": "true"
        }

    return credential


def apply_options(obj, option_dict):
    obj = obj.format("io.github.spark_redshift_community.spark.redshift")
    if len(option_dict) > 0:
        for k,v in option_dict.items():
                obj = obj.option(k,v)
    return obj




def execute_query(egress_config, query):

    credential_dict = parse_credentials(egress_config)

    df_write = spark.createDataFrame([],StructType([StructField('col1',StringType(), True)])).write

    df_write = apply_options(df_write, credential_dict)

    df_write\
        .option("dbtable", 'public.temp_table')\
        .option("preactions", query)\
        .option("postactions", "Drop table public.temp_table;")\
        .mode('overwrite')\
        .save()


def table_creation(df, tbl):
    try:

        new_ddl=''
        for field in df.schema.fields:
            if field.name!='rn':
                if str(type(field.dataType)) == "<class 'pyspark.sql.types.StructType'>":
                    new_ddl = new_ddl + field.name + " " + "SUPER"+", "
                else:
                    new_ddl = new_ddl + field.name + " " + str(field.dataType)+", "
    
        table_ddl = f"""create table if not exists {tbl} ("""+ new_ddl[:-2].replace("StringType","VARCHAR(2000)")\
            .replace("BinaryType", "binary")\
            .replace("ShortType", "SMALLINT")\
            .replace("IntegerType", "INTEGER")\
            .replace("LongType", "BIGINT")\
            .replace("FloatType", val)\
            .replace("DecimalType", val)\
            .replace("DoubleType", "DOUBLE PRECISION")\
            .replace("BooleanType", "BOOLEAN")\
            .replace("TimestampType", "TIMESTAMP") + """)"""
            
    except Exception as e:
        print("Exception while reading schema from Redshift table: ", str(e))

    print(table_ddl)
    return table_ddl


def schema_evolve(egress_config, df):
    query = 'select 1'

    dtype_map = {
            "ShortType" : "SMALLINT",
            "IntegerType" : "INTEGER",
            "LongType" : "BIGINT",
            "FloatType" : val,
            "DoubleType" : val,
            "DecimalType" : val,
            "StringType" : "VARCHAR(65535)",
            "BooleanType" : "BOOLEAN",
            "TimestampType" : "TIMESTAMP",
            "DateType" : "DATE"
        }

    try:
        credential_dict = parse_credentials(egress_config)

        tgt_df = apply_options(spark.read, credential_dict)\
            .option("query",f"select * from {egress_config['target']['options']['dbtable']} limit 5")\
            .load()

        new_col=''
        for field in df.schema.fields:
            if field.name.lower() not in list(map(str.lower,tgt_df.columns)):
                print(f"Adding {field.name} with {str(field.dataType)}")
                new_col=new_col+field.name + " " + str(field.dataType).split('(')[0]+", "

        if new_col != '':
            for datatype in dtype_map.keys():
                new_col = new_col.replace(datatype,dtype_map[datatype])
            query = ''
            for col_nm in new_col[:-2].split(','):
                query += f"alter table {egress_config['target']['options']['dbtable']} add column {col_nm};"

    except Exception as e:
        err_msg = "Exception while reading schema from Redshift table: ", str(e)
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)

    print(query)
    return query



def redshift_stream_write(stream_df, egress_config):
    """
    Writes the streaming dataframe into redshift table.
    required writer/sink details are passed as config

    Parameters
    ----------
    stream_df: Input stream dataframe to be written into Postgresql
    egress_config: required arguments in the form of key-value pairs/options
                for writing into a streaming sink/redshift

    Return
    ------
    None

    """
    try:
        credential_dict = parse_credentials(egress_config)

        source_file_name = egress_config["data"]["inputFile"]["fileName"].replace("/","")


        def foreach_batch_function(df, epoch_id):
            preaction_query = schema_evolve(egress_config, df)

            if egress_config["target"]["options"].get('truncateLoad','') != "":
                mode_option = 'overwrite'
            else:
                mode_option = 'append'

            apply_options(df.write, credential_dict)\
            .option("dbtable", egress_config["target"]["options"]["dbtable"])\
            .option("preactions", preaction_query)\
            .mode(mode_option) \
            .save()


        stream_df = stream_df.writeStream\
            .foreachBatch(foreach_batch_function)\
            .outputMode("append")\
            .option("checkpointLocation", egress_config["target"]["options"]["checkpointLocation"])\
            .trigger(once=True) \
            .queryName(f"DataPersistance-{source_file_name}")\
            .start()

        return stream_df

    except Exception as e:
        err_msg = f"Encountered exception while writing to REDSHIFT - {e}"
        print(err_msg)
        traceback.print_exc()
        raise Exception(e)