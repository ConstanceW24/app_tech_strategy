import sys
from pyspark.sql import SparkSession, utils as spark_utils
from pyspark.sql.functions import (array, coalesce, col, count, current_date,
                                   datediff, explode, expr, length, lit, lower,
                                   regexp_replace, split, substring, sum,
                                   to_date, to_timestamp, trim, udf, when,
                                   from_json,col,split,input_file_name)
from datetime import datetime
import time
spark= SparkSession.builder.getOrCreate()

#method to change column as provided in the config file
def change_column_name(df, ingress_config):
    for i in ingress_config["source"]['datatype_mapping']:
        if 'target_col' in i.keys() and i['target_col'] != '' and i['target_col'] != i['source_col']:
            print("Renaming Source Column {0} with target column {1}".format(i['source_col'],i['target_col']))
            df = df.withColumnRenamed(i['source_col'],i['target_col'])              
    return df 

def reject_handling(df,ingress_config):
    current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    print("performing reject handling")
    reject_records_path = ingress_config["source"]["driver"]["path"]+str(current_timestamp)+"/"
    df = df.option("badRecordsPath",reject_records_path)
    return df


def apply_schema(ingress_config, raw_df):
    if str(ingress_config["data"]["inputFile"].get("schema",None)).lower() not in ( "none", "na") :
        raw_df = raw_df.schema(ingress_config["data"]["inputFile"]["schema"])
    return raw_df

def read_process_check(ingress_config):
    spark= SparkSession.builder.getOrCreate()
    if str(ingress_config["data"]["inputFile"].get('batchFlag','none')).lower() == 'true':
        raw_df = spark.read
    else:
        spark.sql("set spark.sql.streaming.schemaInference=true")
        raw_df = spark.readStream
    return raw_df

def rej_handling(ingress_config, raw_df):
    if str(ingress_config["source"].get("reject_handling","")) != "":
        raw_df = reject_handling(raw_df, ingress_config)
    return raw_df

def col_mapping(ingress_config,raw_df):
    if str(ingress_config["source"].get("column_mapping","")) != "":
        raw_df = change_column_name(raw_df, ingress_config)
    return raw_df


def delta_format_input(ingress_config, raw_df):
    if str(ingress_config["data"]["inputFile"].get("options",{}).get("table","")).lower() not in ( "", "none"):
                # Supports only in databricks 
        raw_df = raw_df.table(ingress_config["data"]["inputFile"]["options"]["table"])

    elif str(ingress_config["source"]["driver"].get("table", "")).lower() not in ( "", "none"):
                # Supports only in databricks 
        raw_df = raw_df.table(ingress_config["source"]["driver"]["table"])
            
    elif '/' not in ingress_config["source"]["driver"]["path"]:
                # Supports only in databricks 
        raw_df = raw_df.table(ingress_config["source"]["driver"]["path"])
    elif str(ingress_config["data"]["inputFile"].get("options",{}).get("path","")).lower() not in ( "", "none"):
        raw_df = raw_df.load()
    else:
        # All Non delta table formats can be accessed through load()
        raw_df = raw_df.load(ingress_config["source"]["driver"]["path"])
    return raw_df



def write_process_check(egress_config, stream_df):
    if str(egress_config["data"]["inputFile"].get('batchFlag','none')).lower() == 'true':
        stream_df = stream_df.write
    else:
        stream_df = stream_df.writeStream
    return stream_df

def select_columns(egress_config, stream_df):
    if "selectCols" in egress_config["target"].get('options', {}).keys():
        stream_df = stream_df.select(*egress_config["target"]['options']['selectCols'])
    
    if "selectQuery" in egress_config["target"].get('options', {}).keys() \
            and egress_config["target"]["configuration"]["data"]["inputFile"].get('batchFlag', 'false').lower() == 'true': 
        temp_tablename = egress_config["configuration"]["table_name"] + (egress_config["configuration"]["run_id"])[:30]
        stream_df.createOrReplaceTempView(temp_tablename)
        qry = egress_config["target"]['options']['selectQuery'].replace("<<table_name>>",temp_tablename)
        stream_df = spark.sql(qry)

    return stream_df 

def get_partition_df(egress_config, stream_df):
    if "noofpartition" in egress_config["target"].keys():
        stream_df = stream_df.coalesce(int(egress_config["target"]["noofpartition"]))

    return stream_df

def apply_options(ingress_config, raw_df):
    kwargs = ingress_config["data"]["inputFile"].get("options",{})
    return apply_to_dataframe(raw_df, kwargs)

def apply_write_options(egress_config, stream_df):
    kwargs=egress_config["target"]["options"]
    return apply_to_dataframe(stream_df, kwargs)


def apply_to_dataframe(df, kwargs):
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    date_YYYYMMDD  = datetime.now().strftime('%Y%m%d')
    date_YYYY_MON_DD  = datetime.now().strftime('%Y-%b-%d')

    if len(kwargs) > 0:
        for k,v in kwargs.items():
            if v == '':
                continue
            if k == "format":
                df = df.format(v)
            elif k == "partitionBy":
                df = df.partitionBy(*(v.strip().split(',')))
            elif k == "schema":
                df = df.schema(eval(v))
            elif k == "mode":
                df = df.mode(v)
            elif (k == 'table') or ('secret' in k) or ('select' in k.lower()) or ('mapcols' in k.lower()) :
                 continue
            else:
                df = df.option(k,v.format(curr_timestamp = timestamp, date_YYYYMMDD = date_YYYYMMDD, date_YYYY_MON_DD = date_YYYY_MON_DD))
    return df


def apply_trigger_option(egress_config, stream_df):
    if egress_config["target"]["targetTrigger"] !='once':
        stream_df=stream_df.trigger(processingTime=egress_config["target"]["targetTrigger"])
    else:
        stream_df=stream_df.trigger(once=True)

    return stream_df

def check_for_delta(egress_config, stream_df):
    if egress_config["target"].get("options","").get("mode","") == "":
        stream_df = stream_df.mode("overwrite")

    if 'delta' in egress_config["target"].get("options","").get("format","") and str(egress_config["target"]["options"].get("table", "")).lower() not in ( "", "none"):
       stream_df = stream_df.saveAsTable(egress_config["target"]["options"]["table"])
    else:
       stream_df = stream_df.save()

def check_target_trigger(egress_config, stream_df):
    from common_utils import utils as utils
    if egress_config["target"]["targetTrigger"] =='once':
        utils.monitor_and_stop_stream(stream_df)




def get_credential_details(ingress_config, options):
    from configs.app_config import get_proj_configs
    project_config           =  get_proj_configs(ingress_config['app_conf'], ingress_config['env'])
    platform = project_config.get("platform", "")

    """
        "secret-scope"          : "kv-cip-<<env>>-eastus",
        "secret-param-url"      : "cip-db2-url",
        "secret-param-port"     : "cip-db2-port",
        "secret-param-driver"   : "cip-db2-driver",
        "secret-param-host"     : "cip-db2-host",
        "secret-param-database" : "cip-db2-database",
        "secret-param-username" : "cip-db2-username",
        "secret-param-password" : "cip-db2-password"
    """
    secret_scope = options.get("secret-scope", "").replace('<<env>>', ingress_config['env'])
    connect_options = {}
    for i in ['url', 'host', 'port', 'driver', 'database', 'username', 'password']:
        if options.get(f"secret-param-{i}", "") != "":
            #secrets population based on platform and producer conf details
            if platform == 'azure':
                from common_utils.utils import get_dbutils
                dbutils = get_dbutils(spark)
                connect_options[i]= dbutils.secrets.get(scope = secret_scope, key = options.get(f"secret-param-{i}", ""))

            elif platform == 'aws':
                #calling secret manager to pull the secrets based on componenet name
                from secrets_utils.secrets_manager import get_secrets_param
                secrets_dict = ""
                if secret_scope != "":
                    secrets_dict = get_secrets_param(secret_scope)
                    if isinstance(secrets_dict, dict):
                        connect_options = secrets_dict
                        break
                    else:
                        connect_options[i]=  get_secrets_param(options.get(f"secret-param-{i}", ""))

    return connect_options


def get_url_for_driver(driver):
    if driver == "com.mysql.jdbc.Driver":  
        url = "jdbc:mysql://{host}:{port}/{database}"  
    elif driver == "org.postgresql.Driver":  
        url = "jdbc:postgresql://{host}:{port}/{database}"  
    elif driver == "oracle.jdbc.driver.OracleDriver":  
        url = "jdbc:oracle:thin:@{host}:{port}:{database}"  
    elif driver == "com.microsoft.sqlserver.jdbc.SQLServerDriver":  
        url = "jdbc:sqlserver://{host}:{port};databaseName={database}"  
    elif driver == "org.sqlite.JDBC":  
        url = "jdbc:sqlite:/{database}"  
    elif driver == "org.apache.derby.jdbc.ClientDriver":  
        url = "jdbc:derby://{host}:{port}/{database}"  
    elif driver == "com.amazon.redshift.jdbc.Driver":  
        url = "jdbc:redshift://{host}:{port}/{database}"  
    elif driver == "org.apache.hive.jdbc.HiveDriver":  
        url = "jdbc:hive2://{host}:{port}/{database}"  
    elif driver == "com.ibm.db2.jcc.DB2Driver":  
        url = "jdbc:db2://{host}:{port}/{database}"
    elif driver == "com.datastax.driver.core.Driver":  
        url = "jdbc:cassandra://{host}:{port}/{database}" 
    else:  
        url = None
    
    print(url)
    return url 

    
def map_connection_options(options_dict):
    keys_to_check = ['host','port','database', 'driver']
    if all(key in options_dict for key in keys_to_check) and 'url' not in options_dict.keys(): 
        url = get_url_for_driver(options_dict['driver'])
        url = url.format(host = options_dict['host'], port = options_dict['port'], database = options_dict['database'])

        dict_keys  = [ key for key in  options_dict.keys() if (key in keys_to_check and key != 'driver') or ('secret-' in key.lower())]
        options_dict['url'] = url
    else:
        dict_keys  = [ key for key in  options_dict.keys() if (key in keys_to_check and key != 'driver') or ('secret-' in key.lower())]
    for key in dict_keys:  
        del options_dict[key]
    
    return options_dict

        
    
