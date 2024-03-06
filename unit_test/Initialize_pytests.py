import requests
import json
import argparse
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate()  

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils 

dbutils = get_dbutils(spark) 

global test_result
test_result={}


def clean_up_tables(ingress_config,dq_config,egress_config):
    """
    Clean up tables and folders created in test runs

    This function is used to drop tables and folders created in test runs 

    Parameter:
        ingress_config
        egress_config
        dq_config
    """
    sfutils=spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils 
    dbutils = get_dbutils(spark)
    dbutils.fs.rm(ingress_config['target']['options']['checkpointLocation'],True)
    dbutils.fs.rm(dq_config['target']['options']['checkpointLocation'],True)
    dbutils.fs.rm(dq_config['target']['options']['path'],True)
    dbutils.fs.rm(egress_config['target']['options']['checkpointLocation'],True)
    raw_tbl = f"""{egress_config["target"]["options"]["dbtable"]}_raw"""
    sf_raw_tbl = f"""{egress_config["target"]["options"]["sfDatabase"]}.{egress_config["target"]["options"]["sfSchema_tgt"]}.{raw_tbl}"""
    sf_raw_stg_tbl= f"""{egress_config["target"]["options"]["sfDatabase"]}.{egress_config["target"]["options"]["sfSchema"]}.{raw_tbl}_staging"""
    merged_tbl = f"""{egress_config["target"]["options"]["dbtable"]}_merged"""
    sf_merged_tbl = f"""{egress_config["target"]["options"]["sfDatabase"]}.{egress_config["target"]["options"]["sfSchema_tgt"]}.{merged_tbl}"""
    stg_tbl = f"""{egress_config["target"]["options"]["dbtable"]}_staging"""
    sf_staging_tbl = f"""{egress_config["target"]["options"]["sfDatabase"]}.{egress_config["target"]["options"]["sfSchema"]}.{stg_tbl}"""
    spark.sql(f"""drop table if exists {raw_tbl}""")
    spark.sql(f"""drop table if exists {merged_tbl}""") 
    sfoptions2 = dict(sfUrl=egress_config["target"]["options"]["sfUrl"],
                               sfUser=egress_config["target"]["options"]["sfUser"],
                               sfPassword=egress_config["target"]["options"]["sfPassword"],
                               sfDatabase=egress_config["target"]["options"]["sfDatabase"],
                               sfSchema=egress_config["target"]["options"]["sfSchema_tgt"],
                               sfWarehouse=egress_config["target"]["options"]["sfWarehouse"])
    sfoptions1 = dict(sfUrl=egress_config["target"]["options"]["sfUrl"],
                               sfUser=egress_config["target"]["options"]["sfUser"],
                               sfPassword=egress_config["target"]["options"]["sfPassword"],
                               sfDatabase=egress_config["target"]["options"]["sfDatabase"],
                               sfSchema=egress_config["target"]["options"]["sfSchema"],
                               sfWarehouse=egress_config["target"]["options"]["sfWarehouse"])
    drop_raw_tbl=f"""Drop table if exists {sf_raw_tbl};"""
    sfutils.runQuery(sfoptions2,drop_raw_tbl)
    drop_merged_tbl=f"""Drop table if exists {sf_merged_tbl};"""
    sfutils.runQuery(sfoptions2,drop_merged_tbl)
    drop_raw_tbl=f"""Drop table if exists {sf_raw_stg_tbl};"""
    sfutils.runQuery(sfoptions1,drop_raw_tbl)
    drop_merged_tbl=f"""Drop table if exists {sf_staging_tbl};"""
    sfutils.runQuery(sfoptions1,drop_merged_tbl)


def expected_dataframe(config,input_type):
    """
    Create expected dataframe for dataplane test functions

    This function is used to Create expected dataframe for dataplane services test functions 

    Parameter:
        config
        input_type
    """
    if input_type=='ingestion':
        input_path=config["source"]["driver"]["path"]
        expected_df=spark.read.format("parquet").load(input_path)
    elif input_type=='transformation':
        source_file_name = config["data"]["inputFile"]["fileName"].replace("/","")
        expected_df = spark.read.format("delta").table(f"{source_file_name}_raw")
    elif input_type=='persistance':
        file_path=config["source"]["driver"]["path"]
        expected_df=spark.sql(f"show columns in {file_path}_merged").toPandas() 
    return expected_df 


