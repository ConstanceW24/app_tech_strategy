import requests
from pyspark.sql import SparkSession, utils as spark_utils
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *
import hashlib

spark= SparkSession.builder.getOrCreate()

def split_path(s3_uri_path):
    
    parse_path = s3_uri_path.split('/')
    if 's3' in parse_path[0].lower():
        bucket = parse_path[2]
        key    = '/'.join(parse_path[3:])
        return bucket, key
    else:
        raise Exception("Not a valid S3 full path")

def get_databricks_content(dbutils, path, json_format=False):
    _=dbutils
    v_dbfs='/dbfs'
    if 'dbfs:/' in path:
        path = v_dbfs + path[5:]
    elif 'dbfs' not in path:
        path = v_dbfs + path
    
    with  open(path, "r") as file:
        contents = file.read()
        
    if json_format:
        import json
        return json.loads(contents)
    return contents


def get_s3_content(s3_client, s3_uri_path, json_format=False):
    
    bucket, key = split_path(s3_uri_path)
    data = s3_client.get_object(Bucket=bucket, Key=key)
    contents = data['Body'].read().decode("utf-8")
    if json_format:
        import json
        return json.loads(contents)
    return contents

def write_databricks_file(dbutils, path, content, json_format=False, mode = 'overwrite'):
    _=dbutils
    v_dbfs='/dbfs'
    if 'dbfs:/' in path:
        path = v_dbfs + path[5:]
    elif 'dbfs' not in path:
        path = v_dbfs + path

    import os
    os.makedirs(os.path.dirname(path), exist_ok=True)
    writemode = 'w' if mode == 'overwrite' else 'a'
    with  open(path, writemode) as file:        
        if json_format:
            import json
            content = json.dumps(content)

        file.write(content)


def write_s3_file(s3_client, s3_uri_path, content, json_format=False, mode = 'overwrite'):
    bucket, key = split_path(s3_uri_path)

    writemode = 'w' if mode == 'overwrite' else 'a'

    if json_format:
        import json
        content = json.dumps(content)
    
    if writemode == 'a':
        existing_content = get_s3_content(s3_client, s3_uri_path, json_format)
        content = existing_content + content

    s3_client.put_object(Bucket=bucket, Key=key, Body=content)

def get_databricks_list(dbutils, path, full_path = True, folders_only = False, with_timestamp = False, recursive = False):
    import os
    file_list = []

    def file_timestamp_format(file_list, path, dir_path, full_path, with_timestamp):
        if full_path != True:
            file_list = file_list + [ (dir_path.path.replace('dbfs:', '').replace(path.replace('dbfs:', ''), ""), os.stat('/' + dir_path.path.replace(':','')).st_mtime) if with_timestamp else dir_path.path.replace('dbfs:', '').replace(path, "")]
        else:
            
            file_list = file_list + [(dir_path.path, os.stat('/' + dir_path.path.replace(':','')).st_mtime) if with_timestamp else dir_path.path ]
        return file_list
    
    for dir_path in dbutils.fs.ls(path):
        if folders_only:
            if (dir_path.isDir() and not recursive):
                file_list = file_timestamp_format(file_list, path, dir_path, full_path, with_timestamp)
            elif dir_path.isDir() and recursive:
                file_list = file_timestamp_format(file_list, path, dir_path, full_path, with_timestamp)
                file_list = file_list + get_databricks_list(dbutils, dir_path.path, full_path, folders_only, with_timestamp, recursive)
            elif dir_path.isFile():
                pass
        else:
            if (dir_path.isFile()) or  (dir_path.isDir() and path != dir_path.path and not recursive):
                file_list = file_timestamp_format(file_list, path, dir_path, full_path, with_timestamp)
            elif dir_path.isDir() and path != dir_path.path and recursive:
                file_list = file_list + get_databricks_list(dbutils, dir_path.path, full_path, folders_only, with_timestamp, recursive)
    
    return file_list


def get_list(dbutils, path, recursive = False):
    return get_databricks_list(dbutils, path, recursive=recursive)

def get_s3_list(s3_client, s3_uri_path, full_path = True, folders_only = False, with_timestamp = False):
    bucket, key = split_path(s3_uri_path)
    s3_prefix   = s3_uri_path.replace(key, '')
    try:
        file_list   = []
        
        if folders_only: 
            file_list = folders_only_list(s3_client, s3_uri_path, full_path, bucket, key, s3_prefix, file_list)
        else:
            obj_list    = s3_client.list_objects_v2( Bucket = bucket, Prefix = key)
            file_list = else_part_file_list(s3_client, s3_uri_path, full_path, bucket, key, s3_prefix, file_list, with_timestamp, obj_list)
    except Exception as e:
        raise Exception("S3 Path doesnot exist or Permission denied : " + str(e))
        
    return file_list

def else_part_file_list(s3_client, s3_uri_path, full_path, bucket, key, s3_prefix, file_list, with_timestamp, obj_list):
    while True:
        for obj in obj_list['Contents']:
            file_nm    =  s3_prefix + obj['Key'] if full_path  else obj['Key'].replace(key,'')
                    
            if file_nm != '' and file_nm != s3_uri_path :
                if with_timestamp:
                    last_modified =  obj['LastModified']
                    file_list  =  file_list + [(file_nm, last_modified)]
                else:
                    file_list  =  file_list + [file_nm]
                        
        if 'NextContinuationToken' in obj_list.keys():
            next_token = obj_list['NextContinuationToken']
            obj_list   = s3_client.list_objects_v2( Bucket = bucket, Prefix = key, ContinuationToken = next_token)
        else:
            break
    return file_list

def folders_only_list(s3_client, s3_uri_path, full_path, bucket, key, s3_prefix, file_list):
    obj_list    = s3_client.list_objects_v2( Bucket = bucket, Delimiter = '/', Prefix = key)
            
    while True:
        for obj in obj_list['CommonPrefixes']:
            file_nm   =  s3_prefix + obj['Prefix'] if full_path  else obj['Prefix'].replace(key,'')
                    
            if file_nm != '' and file_nm != s3_uri_path :
                file_list  =  file_list + [file_nm]
                    
        if 'NextContinuationToken' in obj_list.keys():
            next_token = obj_list['NextContinuationToken']
            obj_list    = s3_client.list_objects_v2( Bucket = bucket, Delimiter = '/', Prefix = key, ContinuationToken = next_token)
        else:
            break
    return file_list

def get_file_list(s3_uri_path, file_list, file_nm):
    if file_nm != '' and file_nm != s3_uri_path :
        file_list  =  file_list + [file_nm]
    return file_list



def clear_s3_files(s3_client, s3_uri_path):
    try:
        bucket, key = split_path(s3_uri_path)
        files_list  = get_s3_list(s3_client, s3_uri_path, full_path = True, folders_only = False)
        key_list    =  [ { 'Key' : '/'.join(i.split('/')[3:])} for i in files_list]
        key_list    =  key_list + [ {'Key': key } ]
        s3_client.delete_objects(Bucket=bucket, Delete={'Objects': key_list })
    except Exception as e:
        print(str(e))

def clear_databricks_files(dbutils, path, recursive = True):
    try:
        for dir_path in dbutils.fs.ls(path):
            if dir_path.isFile():
                remove_path(dbutils, dir_path)
            elif dir_path.isDir() and path != dir_path.path and recursive:
                clear_databricks_files(dbutils, dir_path.path, recursive)
            elif dir_path.isDir() and path != dir_path.path and not recursive:
                remove_path(dbutils, dir_path)
        dbutils.fs.rm(path, True)
    except Exception as e:
        print(str(e))

def remove_path(dbutils, dir_path):
    dbutils.fs.rm(dir_path.path, True)
    print(dir_path.path)

def s3_copy_files(s3_client, src_uri, tgt_uri):
    try:
        file_list = get_s3_list(s3_client, src_uri)
        for file in file_list:
            source_bkt, source_key = split_path(file)
            target_bkt, target_key = split_path(file.replace(src_uri,tgt_uri))
            s3_client.copy({"Bucket": source_bkt, "Key": source_key}, target_bkt, target_key) 
        return True
    except Exception as e:
        print(f"Exception in s3_copy_files {e}")
        import traceback
        print(traceback.format_exc())
        return False

def s3_move_files(s3_client, src_uri, tgt_uri):
    status = s3_copy_files(s3_client, src_uri, tgt_uri)
    if status:
        clear_s3_files(s3_client, src_uri)

def databricks_copy_files(dbutils, src_path, tgt_path, recursive = True):
    dbutils.fs.cp(src_path, tgt_path, recurse=recursive)

def databricks_move_files(dbutils, src_path, tgt_path, recursive = True):
    dbutils.fs.mv(src_path, tgt_path, recurse=recursive)


def monitor_and_stop_stream(df):
    df.awaitTermination()
    print(df.lastProgress)
    print(df.status)





def fix_headers(df):
    import re
    """
    to replace non alpha-numeric characters in column names with undersocre

    Parameters
    ----------
    df : a spark dataframe

    Output
    ------
    df : a spark dataframe with changed column names
    """

    print("Fixing column headers")

    sc_replace = lambda x: re.sub('[^A-Za-z0-9]+', '_', x.strip())
    br_replace = lambda x: re.sub(r"[\([{})\]]", "", x.strip())
    multi_replace = lambda x: sc_replace(br_replace(x))
    columns = df.schema.names
    columns = {c: multi_replace(c) for c in columns}
    for k,v in columns.items():
        df = df.withColumnRenamed(k,v)
    
    return df

def drop_columns(df, cols_var):
    columns = df.schema.names
    
    if str(type(cols_var)) == "<class 'str'>":
        columns = [c for c in columns if cols_var in c] 
    elif str(type(cols_var)) == "<class 'list'>":
        columns = cols_var
    else:
        raise Exception("Input cols_var should be string or list")
        
    print(f"Dropping columns {columns}")
    df = df.drop(*columns)
    return df

def get_config_json(config_id, init=False, init_endpoint='20.85.191.136', config_endpoint='20.85.191.192'):
    
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    if init:
        url = f"http://{init_endpoint}/data-initialization-catalog/initialization/"
    else:
        url = f"http://{config_endpoint}/data-config-catalog/config/"
    
    print(url + config_id)
    response = requests.get(url + config_id, headers=headers)

    return response.json()

def add_partition_to_catalog(config):
    try:
        import boto3
        glue_client = boto3.client('glue', region_name='us-east-1')
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_location = config['StorageDescriptor']['Location']
        partition_list = []
        try:
            partition_list  = [part.replace('/', '') for part in get_s3_list(s3_client, s3_location, full_path = False, folders_only = True)]
            print('partition_list: ', partition_list)
        except Exception as e:
            print(f'Skipping add_partition_to_catalog : {e}')
            partition_list = []
        for partition_value in partition_list:
            print('Adding partition_value: ', partition_value)
            partition_location = s3_location + f"{partition_value}/"
            config['StorageDescriptor']['Location'] = partition_location
            try:
                resp = glue_client.create_partition(
                DatabaseName= config['DatabaseName'],
                TableName=config['TableName'],
                PartitionInput={
                    'Values': [partition_value],
                    'StorageDescriptor': config['StorageDescriptor']
                }
                )
            except glue_client.exceptions.AlreadyExistsException as aee:
                print(f'Error: {aee}')
                print(f'partition {partition_value} already exists so skipping add_partition_to_catalog..{resp}')
            config['StorageDescriptor']['Location'] = s3_location
    except Exception as e:
        raise ValueError(f"Error in add_partition_to_catalog : {e}")
    
def type_cols(df_dtypes, filter_type):
    cols = []
    for col_name, col_type in df_dtypes:
        if col_type.startswith(filter_type):
            cols.append(col_name)
    return cols

def df_flatten_structs_attr(struct_df, exp_col=""):
    """
    to flatten the struct type columns in a xml dataframe,
    if the explode column name is provided only flatten related columns, if not, flatten all struct type columns
    Parameters
    ----------
    nested dataframe
    explode column name
    ----------
    Returns flatten df 
    """

    stack = [((), struct_df)]
    columns = []
    while len(stack) > 0:
        parents, df = stack.pop()
        flat_cols = [col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
                    for c in df.dtypes if not c[1].startswith("struct") or (exp_col != "" and not c[0].startswith(exp_col))]
        nested_cols = [c[0] for c in df.dtypes 
                        if (c[1].startswith("struct") and exp_col != "" and c[0].startswith(exp_col))
                        or (c[1].startswith("struct") and exp_col == "")]
        columns.extend(flat_cols)
        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))
        
    return struct_df.select(columns)


def df_flatten_attr(df, identifier=0, exp_col=""):
    """
    to flatten/explode the array type columns in a xml dataframe, 
    if the explode column name is provided only flatten related columns, if not, flatten all array type columns 
    also calls the struct flatten function to handle the struct type columns inside it
    Parameters
    ----------
    nested dataframe
    identifier
    explode column name
    ----------
    Returns flatten df 
    """
    array_cols = [c[0] for c in df.dtypes if c[1].startswith("array") and ((exp_col != "" and c[0].startswith(exp_col)) or exp_col == "")]
    nested_cols = [c[0] for c in df.dtypes if c[1].startswith("struct") and ((exp_col != "" and c[0].startswith(exp_col)) or exp_col == "")]

    while len(array_cols) > 0 or len(nested_cols) > 0:
        for array_col in array_cols: 
            if array_col.startswith(exp_col) or identifier == 0:
                df = df.withColumn(array_col, explode_outer(col(array_col)))
            else:
                df = df.withColumn(array_col, explode(col(array_col)))
                
        df = df_flatten_structs_attr(df, exp_col)

        array_cols = [c[0] for c in df.dtypes if c[1].startswith("array") and ((exp_col != "" and c[0].startswith(exp_col)) or exp_col == "")]
        nested_cols = [c[0] for c in df.dtypes if c[1].startswith("struct") and ((exp_col != "" and c[0].startswith(exp_col)) or exp_col == "")]

    return df
        
# Function to remove spaces from column names to write into delta table
def default_column_rename(df):
    replace_str=' '
    replace_with='_'
    for column_name in df.schema.fieldNames():
         df=df.withColumnRenamed(column_name,column_name.replace(replace_str,replace_with))
    return df

def read_config_from_json(file_path):  
    import json
    with open(file_path, 'r') as file:  
        config = json.load(file)  
    return config  


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        try:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        except Exception as e:
            print(f'Exception Occured : {e}')
            dbutils = None
    return dbutils

def apply_request_header(fsm_ers_header, request_header):
    request_header = {k.lower() : v for k,v in request_header.items()}
    return {k: request_header[k.lower()] if k.lower() in request_header else v for k, v in fsm_ers_header.items()}  

def string_to_hex(input_string):
    hash_object = hashlib.sha256(str(input_string).encode())     
    return hash_object.hexdigest()

