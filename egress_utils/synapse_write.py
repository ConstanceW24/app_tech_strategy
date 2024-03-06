import datetime
from pyspark.sql import SparkSession
import json
import sys
import os
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
from pyspark.sql.functions import col, to_json, lit, struct, concat, collect_list, concat_ws

spark = SparkSession.builder.getOrCreate()

val = "DECIMAL(38,8)"

def parse_credentials(egress_config):

    if egress_config["target"]["options"].get('url','') != "":
        jdbcurl = egress_config["target"]["options"]["url"]
    
    elif egress_config["target"]["options"].get('host','') != "":

        #Reading the Data Persistance Config Configurations
        host     = egress_config["target"]["options"]['host']
        port     = egress_config["target"]["options"].get('port', 1433)

        if egress_config["target"]["options"].get('password','') != "":
            username = egress_config["target"]["options"]["username"]
            password = egress_config["target"]["options"]["password"]       
            jdbcurl="jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(host, port, egress_config["target"]["options"].get("database",'sqlpoolccsdeveastus'), username, password)
        else:
            jdbcurl="jdbc:sqlserver://{0}:{1};database={2}".format(host, port, egress_config["target"]["options"].get("database",'sqlpoolccsdeveastus'))

    credential = {
        "url": jdbcurl,
        "tempDir": egress_config["target"]["options"]["tempdir"],
        "enableServicePrincipalAuth":"true",
        "useAzureMSI":"true"
        }

    return credential


def apply_options(obj, option_dict):
    obj = obj.format("com.databricks.spark.sqldw")
    if len(option_dict) > 0:
        for k,v in option_dict.items():
                obj = obj.option(k,v)
    return obj




def execute_query(egress_config, query):

    credential_dict = parse_credentials(egress_config)

    df_write = spark.createDataFrame([],StructType([StructField('col1',StringType(), True)])).write

    df_write = apply_options(df_write, credential_dict)

    df_write\
        .option("dbTable", 'dbo.temp_table')\
        .option("preActions", query)\
        .option("postActions", "Drop table dbo.temp_table;")\
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
            .replace("DoubleType", val)\
            .replace("BooleanType", "BIT")\
            .replace("TimestampType", "DATETIME") + """)"""
            
    except Exception as e:
        print("Exception while reading schema from Synapse table: ", str(e))

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
            "StringType" : "VARCHAR(2000)",
            "BooleanType" : "BIT",
            "TimestampType" : "DATETIME",
            "DateType" : "DATE"
        }

    try:
        credential_dict = parse_credentials(egress_config)

        tgt_df = apply_options(spark.read, credential_dict)\
            .option("query",f"select TOP 5 * from {egress_config['target']['options']['dbTable']} ")\
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
                query += f"alter table {egress_config['target']['options']['dbTable']} add column {col_nm};"

    except Exception as e:
        print("Exception while reading schema from Synapse table: ", str(e))

    print(query)
    return query



def synapse_stream_write(stream_df, egress_config):
    """
    Writes the streaming dataframe into synapse table.
    required writer/sink details are passed as config

    Parameters
    ----------
    stream_df: Input stream dataframe to be written into Synapse
    egress_config: required arguments in the form of key-value pairs/options
                for writing into a streaming sink/Synapse

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
            .option("preActions", preaction_query)\
            .option("dbTable", egress_config["target"]["options"]["dbTable"])\
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
        print(f"Encountered exception during Persistance of the data - {e}")
        sys.exit(1)