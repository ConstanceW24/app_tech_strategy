from pyspark.sql import SparkSession 
from common_utils.utils import get_dbutils
import traceback
from pyspark.sql.functions import col, to_json, lit, struct, concat, collect_list, concat_ws

spark= SparkSession.builder.getOrCreate()




field_datatype = "<class 'pyspark.sql.types.StructType'>"
varchartype    = "Varchar(16777216)"
numbertype     = "NUMBER(38,0)"

sfutils=spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils


def sf_table_creation(table_name,sf_table_name,sfoptions):
    """
    sf_table_creation

    This function is used to create the snowflake tables for the first time

    Parameter:
        table_name
        sf_table_name
        sfoptions
    """
    sfutils=spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils
    tbl_df=spark.sql(f"""select * from {table_name}""")
    new_ddl=''
    for field in tbl_df.schema.fields:
        if field.name!='rn':
            if str(type(field.dataType))==field_datatype:
                new_ddl=new_ddl+field.name + " " + "VARIANT"+", "
            else:
                new_ddl=new_ddl+field.name + " " + str(field.dataType)+", "

    sf_table_ddl = f"""create table if not exists {sf_table_name} ("""+ new_ddl[:-2].replace("StringType",varchartype).replace("BinaryType","binary").replace("IntegerType",numbertype).replace("DecimalType","NUMBER").replace("LongType",numbertype).replace("DoubleType","DOUBLE").replace("BooleanType","BOOLEAN").replace("TimestampType","TIMESTAMP") + """)"""
    sfutils.runQuery(sfoptions,sf_table_ddl)


def sf_schema_evolve(tbl_nm, src_options, tgt_table_options):
    """
    schema_evolve_raw

    This function checks for schema evolution in delta layer and replicates the same in snowflake raw layer in a additive manner

    Parameter:
        table
        src connection
        {targettable: target connection}
    """
    sfutils=spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils
    raw_tbl_df=spark.sql(f"""select * from {tbl_nm}""")
    existing_tgt_schema_raw=spark.read.format("net.snowflake.spark.snowflake").options(**src_options).option("dbtable",tbl_nm).load()
    new_col=''
    for field in raw_tbl_df.schema.fields:
        if field.name.upper() not in existing_tgt_schema_raw.columns:
            if str(type(field.dataType))==field_datatype:
                new_col=new_col+field.name + " " + "VARIANT"+", "
            else:
                new_col=new_col+field.name + " " + str(field.dataType)+", "

    if  new_col!='':
        
        for key in tgt_table_options.keys():
            alter_query_raw=f"""alter table {key} add column """ + new_col[:-2].replace("StringType",varchartype).replace("BinaryType","binary").replace("IntegerType",numbertype).replace("DecimalType","NUMBER").replace("LongType",numbertype).replace("DoubleType","DOUBLE").replace("BooleanType","BOOLEAN").replace("TimestampType","TIMESTAMP")
            sfutils.runQuery(tgt_table_options[key], alter_query_raw)



def merge_query_generate(merged_tbl,sf_merged_tbl,sf_staging_tbl):
    """
    merge_query_generate

    This function generates the merge query dynamically

    Parameter:
        merged_tbl
        sf_staging_tbl
        sf_merged_tbl
    """
    column_df=spark.sql(f"show columns in {merged_tbl}").filter("col_name <>'rn'")
    df_query=column_df.withColumn("update_cols",concat(lit('TARGET.'),col('COL_NAME'),lit('=STAGING.'),col('COL_NAME'))).\
                withColumn("insert_cols",concat(lit('STAGING.'),col('COL_NAME'))).\
                select(concat_ws(',',collect_list(col('update_cols'))).alias('update_columns'),\
                    concat_ws(',',collect_list(col('insert_cols'))).alias('insert_values'),\
                    concat_ws(',',collect_list(col('COL_NAME'))).alias('insert_columns')).\
                withColumn("update_query",concat(lit('Update SET '),col('update_columns'))).\
                withColumn("insert_query",concat(lit('INSERT('),col('insert_columns'),lit(')'),lit('VALUES('),col('insert_values'),lit(')'))).\
                select('insert_query','update_query').toPandas()
    insert_query=list(df_query['insert_query'])[0]
    update_query=list(df_query['update_query'])[0]

    merge_query=f"""MERGE INTO {sf_merged_tbl} TARGET
                    USING (SELECT * FROM
                                (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY cda_timestamp DESC,gwcbi___seqval_hex DESC,gwcbi___lsn desc)
                                AS rn_new FROM {sf_staging_tbl}) WHERE rn_new=1) STAGING
                    ON
                        TARGET.ID=STAGING.ID
                    WHEN MATCHED
                        THEN {update_query}
                    WHEN NOT MATCHED
                        THEN {insert_query};"""
    return merge_query



# DBTITLE 1,Filestream Class Definition
def snowflake_stream_write(stream_df,mode,output_mode,dbtable,egress_config,checkpoint,query):        
    """
    Writes the streaming dataframe into snoflake in relational table format
    
    
    Parameters
    ----------
    stream_df: Input stream dataframe to be written into snowflake
    mode/outmode: Append to add the records into existing table
    dbtable:table name
    egress_config: required arguments in the form of key-value pairs/options
                for writing into a streaming sink
    checkpoint: streaming checkpoint location
         
    
    Return
    ------
    Returns the streaming dataframe
    
    """
    try:

    
        if 'guidewire' not in egress_config["data"]["eventTypeId"].lower():
            dbutils      = get_dbutils(spark)
            sfurl        = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-sf-url")
            sfuser       = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-sf-username")
            sfpassword   = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-sf-pwd")
            sfdatabase   = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-sf-database")
            sfschema_tgt = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-sf-schema-tgt")


            sfoptions = dict(
                            sfUrl=sfurl,
                            sfUser=sfuser,
                            sfPassword=sfpassword,
                            sfDatabase=sfdatabase,
                            sfSchema=sfschema_tgt
                            )
        else:
            sfoptions = dict(sfUrl=egress_config["target"]["options"]["sfUrl"],
                            sfUser=egress_config["target"]["options"]["sfUser"],
                            sfPassword=egress_config["target"]["options"]["sfPassword"],
                            sfDatabase=egress_config["target"]["options"]["sfDatabase"],
                            sfSchema=egress_config["target"]["options"]["sfSchema"],
                            sfWarehouse=egress_config["target"]["options"]["sfWarehouse"])
        sfutils=spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils


        def foreach_batch_function(stream_df,epoch_id):
            """
            Gets the streaming dataframe as mini batch dataframe with unique batch id
            """

            stream_df.write.format("net.snowflake.spark.snowflake") \
                .options(**sfoptions) \
                .option("dbtable", dbtable) \
                .mode(mode) \
                .save()
            if query !="No query": 
                sfutils.runQuery(sfoptions,query)

        stream_final_df=stream_df.coalesce(int(egress_config["target"]["noofpartition"])).writeStream\
                        .format("delta")\
                        .foreachBatch(foreach_batch_function)\
                        .outputMode(output_mode)\
                        .option("checkpointLocation", checkpoint)
        
        if egress_config["target"]["targetTrigger"] !='once':
            stream_final_df=stream_final_df.trigger(processingTime=egress_config["target"]["targetTrigger"])
        else:
            stream_final_df=stream_final_df.trigger(once=True)
        stream_final_df.start()  
    except Exception as e:
        err_msg = f"Encountered exception while writing to SNOWFLAKE - {e}"
        print(err_msg)
        raise Exception(e)
        traceback.print_exc()