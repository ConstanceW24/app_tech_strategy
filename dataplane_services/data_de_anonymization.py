"""
    Data De Anonymization Service - This Service brings back the original value for the masked values for a specific group of users and copy the data to a specific location.
"""
import sys
import traceback
from pyspark.sql import SparkSession
from copy import deepcopy
from pyspark.sql.functions import *
from common_utils import utils
from common_utils.batch_writer import batch_writer
from ingress_utils import blob_read, s3_read
from ccs_publisher import ccs_publisher as ccs_dp
from secrets_utils.secrets_manager import get_secrets, get_secrets_param
import os
from pyspark.sql.window import Window
import functools

spark= SparkSession.builder.getOrCreate()

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

dbutils = get_dbutils(spark)

def source_df_read(ingress_config):
    if ingress_config["source"]["driver"]["SourceType"] =='s3':
        df_stream = s3_read.s3_data_read(ingress_config)

    if ingress_config["source"]["driver"]["SourceType"]=='blob':
        df_stream = blob_read.blob_data_read(ingress_config)
        
    return df_stream

def apply_df_options(kwargs, df_mapping):
    if len(kwargs) > 0:
        for k,v in kwargs.items():
            df_mapping = df_mapping.option(k,v)
    return df_mapping

def demask_col_qry(columns_to_demask, query_deanonymized, demasklen):
    for mcol in columns_to_demask:
        query_deanonymized = query_deanonymized + f'mpt.{mcol}_original AS {mcol}'
        if demasklen>1:
            query_deanonymized = query_deanonymized + ", "
        demasklen = demasklen-1
    return query_deanonymized

def prmry_key_qry_da(prim_key, query_deanonymized, join_qry_da, plen):
    for pkey in prim_key:
        query_deanonymized = query_deanonymized + f'src.{pkey}, '
        join_qry_da = join_qry_da + f"COALESCE(src.{pkey},'ZZZZ') = COALESCE(mpt.{pkey},'ZZZZ')"
        if plen > 1:
            join_qry_da = join_qry_da + " AND "
        plen = plen-1
    return query_deanonymized,join_qry_da

def fernet_encrypt_key():
    if(os.getenv('platform')=='azure'):
            encryption_key = dbutils.secrets.get(scope = "ccs-datapipeline-akv-secrets", key = "ccs-dp-enc-master-key")
    if(os.getenv('platform')=='aws'):
            encryption_key= get_secrets_param("FernetEncryption")['encryption_master_key']
    return encryption_key

def other_col_qry(all_columns, query_deanonymized, aclen):
    if aclen >= 1:
        query_deanonymized = query_deanonymized + ','
    for mcol in all_columns:
        query_deanonymized = query_deanonymized + f'src.{mcol}'
        if aclen>1:
            query_deanonymized = query_deanonymized + ", "
        aclen = aclen-1
    return query_deanonymized

def prim_key_check(prim_key):
    if len(prim_key) == 0:
        raise ValueError("Primary keys not provided in the configuration")

def fernet_decrypt_loop(fernet_col, masked_df):
    def fernet_decrypt(colval,master_key):
        from cryptography.fernet import Fernet
        f = Fernet(master_key)
        if colval:
            colval_b = bytes(colval, 'utf-8')
            decipher_val = f.decrypt(colval_b)
            decipher_va = str(decipher_val.decode('ascii'))
            return decipher_va
        else:
            return colval
    frnt_dcrypt_udf = udf(fernet_decrypt, StringType())
    for frntcol in fernet_col:
        encryption_key = fernet_encrypt_key()
        encryption_key = bytes(encryption_key, 'utf-8')
        masked_df=masked_df.withColumn(frntcol,frnt_dcrypt_udf(frntcol, lit(encryption_key)))
    return masked_df

def data_de_anonymization(ingress_config):
    """
        De Anonymize the source data using the Masking mapping table and write the data per rules specified, should be used along with Masking dataplane service.
    """

    #setting variable for queries
    source_file_name    = ingress_config["data"]["inputFile"]["fileName"].replace("/","")
    process_name        = f"DataDeAnonymization-{source_file_name}"

    # Validating Configuration
    config.valid_config_check(ingress_config, process_name)

    try:
        # publish the DP state to FSM topic
        ccs_dp.dp_ccs_publisher(('DATA DE-ANONYMIZATION STARTED'),'fsm')

        ############ Reading the masked source data ###################
        df_src_stream = source_df_read(ingress_config)

        ############ Reading the batch mapping table data for source ###################
        kwargs = ingress_config["data"]["inputFile"]["options"]
        df_mapping = (spark            
                    .read
                    .format(ingress_config["data"]["inputFile"]["inputFileFormat"])
                    )
        df_mapping = apply_df_options(kwargs, df_mapping)
        df_mapping = df_mapping.load((ingress_config["source"]["driver"]["path"].replace("data_masked", "masking_mapping_table")))
        wndw_lst_rcrd = Window.partitionBy(functools.reduce(lambda a, b: a + "," + b, ingress_config["data"]["inputFile"]["primary_key"])).orderBy(desc("LoadTimeStamp"))
        df_mapping = df_mapping.withColumn("row_num",row_number().over(wndw_lst_rcrd))
        df_mapping = df_mapping.where(df_mapping.row_num=='1').drop("row_num")
        df_mapping.createOrReplaceTempView("df_mapping_table")

        ############################################################################

        def foreach_deanonymization(maskeddf, batchid):
            for rule in ingress_config["rules"]:
                ############ Fernet Decryption ###################
                fernet_col = []
                masked_df = maskeddf
                if rule["fernet_field_name"]:
                    fernet_col = fernet_col + list(rule["fernet_field_name"].split(","))
                    masked_df = fernet_decrypt_loop(fernet_col, masked_df) 

                spark.sql("""DROP TABLE IF EXISTS df_masked_src_table""")
                masked_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("df_masked_src_table")

                prim_key = ingress_config["data"]["inputFile"]["primary_key"]
                columns_to_demask = list(rule["demask_field_name"].split(","))
                all_columns = masked_df.columns
                for col in (prim_key + columns_to_demask):
                    all_columns.remove(col)
                
                demask_group = rule["demask_group_name"]
                target_path = ingress_config["target"]["options"]["path"] + demask_group + "/"

                query_deanonymized = "SELECT "
                join_qry_da = ""
                plen = len(prim_key)
                prim_key_check(prim_key)
                query_deanonymized, join_qry_da = prmry_key_qry_da(prim_key, query_deanonymized, join_qry_da, plen)
                demasklen = len(columns_to_demask)
                query_deanonymized = demask_col_qry(columns_to_demask, query_deanonymized, demasklen)
                aclen = len(all_columns)
                query_deanonymized = other_col_qry(all_columns, query_deanonymized, aclen)
                query_deanonymized = query_deanonymized + f" FROM df_masked_src_table src LEFT JOIN df_mapping_table mpt ON {join_qry_da}"

                df_unmasked_data = spark.sql(query_deanonymized)
                df_unmasked_data = df_unmasked_data.select(*masked_df.columns)
                df_unmasked_data.write.format(ingress_config["target"]["options"]["format"]).option("header", "true").mode("append").option("path", target_path).save()

        if ingress_config["target"]["targetTrigger"] !='once':
            df_src_stream.writeStream\
                .foreachBatch(foreach_deanonymization)\
                .outputMode("append")\
                .option("checkpointLocation", ingress_config["target"]["options"]["checkpointLocation"])\
                .trigger(processingTime=ingress_config["target"]["targetTrigger"])\
                .start()
        else:
            df_src_stream.writeStream\
                .foreachBatch(foreach_deanonymization)\
                .outputMode("append")\
                .option("checkpointLocation", ingress_config["target"]["options"]["checkpointLocation"])\
                .trigger(once=True)\
                .start()\
                .awaitTermination()
            
        # publish the DP state to FSM topic
        ccs_dp.dp_ccs_publisher(('DATA DE-ANONYMIZATION COMPLETED'),'fsm')
        
    except Exception as e:
        err_msg = f"Error during Data De Anonymization Service - {e}"
        print(err_msg)
        ccs_dp.dp_ccs_publisher(('DATA DE-ANONYMIZATION FAILED'),'fsm')
        ccs_dp.dp_ccs_publisher(('DATA DE-ANONYMIZATION FAILED',err_msg),'ers')
        traceback.print_exc()
        sys.exit(1)