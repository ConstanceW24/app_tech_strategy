import sys
from pyspark.sql import SparkSession, utils as spark_utils, DataFrame
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from egress_utils import delta_write
from common_utils import  utils as utils
from common_utils.batch_writer import batch_writer
from common_utils.custom_handler import *
from data_quality_validations import *
from dataplane_services.data_quality import *
from copy import deepcopy
from delta.tables import *

spark= SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")


# Upsert To Delta Merge Operation method
def upsert_merge_tranformation(source_file_name,src,ingress_config):
    """
    to perform merge trnasformation in microbatch and wirte the data in delta layer

    Parameters
    ----------
    ingress_config
    src
    source_file_name

    Output
    ------
    Writes the data in specified delta table

    """
    def upsert_to_delta_gw(microbatchoutputdf, batchid):
        stg_tbl = f"{source_file_name}_stg"
        merged_tbl = f"{source_file_name}_merged"
        microbatchoutputdf.createOrReplaceTempView(stg_tbl)
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {merged_tbl} USING DELTA AS SELECT *, CAST('0' as int) AS rn, cast("" as string) as CDA_IsDeleted FROM {source_file_name}_raw WHERE 1=2""")
        #spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")
        microbatchoutputdf._jdf.sparkSession().sql(f"""
        MERGE INTO {merged_tbl} t
        USING (SELECT * FROM ( SELECT *,
                                ROW_NUMBER() OVER (PARTITION BY id ORDER BY cda_timestamp desc, gwcbi___seqval_hex DESC,gwcbi___lsn desc) AS rn ,
                                case when gwcbi___operation=1 then "Y" else null end as CDA_IsDeleted
                                FROM {stg_tbl}
                                )
                                WHERE rn=1
                ) s
        ON s.id = t.id
        WHEN MATCHED AND (s.gwcbi___seqval_hex>t.gwcbi___seqval_hex  and s.gwcbi___lsn>t.gwcbi___lsn)  THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
            """)

    ########### Write the Stream into delta ##########
    if ingress_config["target"]["targetType"] in ['delta']:

        final_df = delta_write.delta_data_write(src,ingress_config)

        stream_df = final_df.queryName(f"DataTransformation-{source_file_name}")\
                .foreachBatch(upsert_to_delta_gw) \
                .start()

        if ingress_config["target"]["targetTrigger"] =='once':
            utils.monitor_and_stop_stream(stream_df)

def stream_scd1_merge_transformation(src, src_name, mergecheck_col, ingress_config):
    """
    to perform SCD1 transformation on a streaming dataframe
    Parameters
    ----------
    src - source dataframe
    src_name - base table name or file name, used to create tables
    mergecheck_col - list of columns on which merge will be checked
    ingress_config
    ----------
    Writes the streaming data in a specified delta table using the correct path and targettype
    """
    def stream_merge_scd1(src_df,batchid):
        stg_tbl = f"{src_name}_stg"
        lnd_tbl = f"{src_name}_cur"
        targetpath = ingress_config["target"]["options"]["path"]
        merge_check_query = ""
        partitionby_cols = ""
        y = len(mergecheck_col)
        for x in mergecheck_col:
            merge_check_query = merge_check_query + f"lnd.{x} = stg.{x}"
            partitionby_cols = partitionby_cols + x
            if y>1:
                merge_check_query = merge_check_query + " AND "
                partitionby_cols = partitionby_cols + ", "
            y=y-1
            
        spark.sql(f"""DROP TABLE IF EXISTS {stg_tbl}""")
        src_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(stg_tbl)
        
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {lnd_tbl} 
                                   USING DELTA LOCATION '{targetpath}' AS SELECT * FROM {stg_tbl} WHERE 1=2""")
        src_df._jdf.sparkSession().sql(f"""
                                            MERGE INTO {lnd_tbl} lnd
                                            USING (SELECT * FROM {stg_tbl}
                                                   QUALIFY ROW_NUMBER() OVER(PARTITION BY {partitionby_cols} ORDER BY load_timestamp DESC) = 1) stg
                                            ON {merge_check_query}
                                            WHEN MATCHED THEN UPDATE SET *
                                            WHEN NOT MATCHED THEN INSERT *
                                        """)

    stream_df = src.writeStream\
                .outputMode("append")\
                .option("checkpointLocation", ingress_config["target"]["options"]["checkpointLocation"])\
                .queryName(f"DataTransformation-{src_name}")\
                .foreachBatch(stream_merge_scd1) 

    if ingress_config["target"]["targetTrigger"] !='once':
        stream_df = stream_df.trigger(processingTime=ingress_config["target"]["targetTrigger"])\
            .start()
    else:
        stream_df = stream_df.trigger(once=True)\
            .start()\
            .awaitTermination()

def batch_scd1_merge_transformation(src,src_name,mergecheck_col,ingress_config):
    """
    to perform SCD1 transformation on a batch dataframe
    Parameters
    ----------
    src - source dataframe
    src_name - base table name or file name, used to create tables
    mergecheck_col - list of columns on which merge will be checked
    ingress_config
    ----------
    Writes the data in a specified delta table using the correct path
    """
    stg_tbl = f"{src_name}_stg"
    lnd_tbl = f"{src_name}_cur"
    targetpath = ingress_config["target"]["options"]["path"]
    merge_check_query = ""
    partitionby_cols = ""
    y = len(mergecheck_col)
    for x in mergecheck_col:
        merge_check_query = merge_check_query + f"lnd.{x} = stg.{x}"
        partitionby_cols = partitionby_cols + x
        if y>1:
            merge_check_query = merge_check_query + " AND "
            partitionby_cols = partitionby_cols + ", "
        y=y-1

    spark.sql(f"""DROP TABLE IF EXISTS {stg_tbl}""")
    src.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(stg_tbl)
    
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {lnd_tbl} 
                               USING DELTA LOCATION '{targetpath}' AS SELECT * FROM {stg_tbl} WHERE 1=2""")
    src._jdf.sparkSession().sql(f"""
                                   MERGE INTO {lnd_tbl} lnd
                                   USING (SELECT * FROM {stg_tbl}
                                          QUALIFY ROW_NUMBER() OVER(PARTITION BY {partitionby_cols} ORDER BY load_timestamp DESC) = 1) stg
                                   ON {merge_check_query}
                                   WHEN MATCHED THEN UPDATE SET *
                                   WHEN NOT MATCHED THEN INSERT *
                                 """)

def msi_base_ignored_fields(ignored_fields, ignore_tag_list):
    for d in ignore_tag_list:
        if d.split(":")[0].strip() == 'root':
            ignored_fields.append(d.split(":")[1])
        for e in ignored_fields:
            ignored_fields = e.split(",")
        ignored_fields = [f.strip() for f in ignored_fields]
    return ignored_fields

def msi_base_scd1_normal_call(ingress_config, basefilename, process_name, src_base_df, scd1_field):
    if len(scd1_field) > 0:
        print("scd1 Transformation Started for Base table")
        stream_scd1_merge_transformation(src_base_df,basefilename,scd1_field,ingress_config)
    else:
        stream.file_storage_write_stream(process_name, src_base_df, ingress_config)

def msi_base_flatten_call(src_base_df):
    if len(utils.type_cols(src_base_df.dtypes, "array")) != 0:
        src_base_df = utils.df_flatten_attr(src_base_df,0)
    else :
        src_base_df = utils.df_flatten_structs_attr(src_base_df)
    return src_base_df

def msi_tag_parentcol_list(tags, parent_cols,split_tag_list):
    for k in split_tag_list:
        if k.split(":")[0].strip() == tags:
            parent_cols.append(k.split(":")[1])
        for l in parent_cols:
            parent_cols = l.split(",")
        parent_cols = [m.strip() for m in parent_cols]
    return parent_cols

def msi_tag_dropcol_list(tags, drop_cols,ignore_tag_list):
    for p in ignore_tag_list:
        if p.split(":")[0].strip() == tags:
            drop_cols.append(p.split(":")[1])
        for q in drop_cols:
            drop_cols = q.split(",")
        drop_cols = [r.strip() for r in drop_cols]
    return drop_cols

def msi_tag_scd1_list(tags, scd1_tag_field,scd1_tag_list):
    for x in scd1_tag_list:
        if x.split(":")[0].strip() == tags:
            scd1_tag_field.append(x.split(":")[1])
        for w in scd1_tag_field:
            scd1_tag_field = w.split(",")
        scd1_tag_field = [z.strip() for z in scd1_tag_field]
    return scd1_tag_field

def msi_tag_scd1_normal_call(tags, src_split_df, ingress_config_tag, basefilename_tag, scd1_tag_field):
    if len(scd1_tag_field) > 0:
        print("scd1 Transformation Started for "+ tags + " table")
        batch_scd1_merge_transformation(src_split_df,basefilename_tag,scd1_tag_field, ingress_config_tag)
    else:
        src_split_df.write.format("delta").option("mergeSchema", "true").mode("append").option("path", ingress_config_tag["target"]["options"]["path"]).save()

def msi_tag_flatten_call(src_split_df):
    if len(utils.type_cols(src_split_df.dtypes, "array")) != 0:
        src_split_df = utils.df_flatten_attr(src_split_df,1)
    else :
        src_split_df = utils.df_flatten_structs_attr(src_split_df)
    return src_split_df

def msi_base_scd1_list(scd1_field, scd1_tag_list):
    for h in scd1_tag_list:
        if h.split(":")[0].strip() == 'root':
            scd1_field.append(h.split(":")[1])
        for i in scd1_field:
            scd1_field = i.split(",")
        scd1_field = [j.strip() for j in scd1_field]
    return scd1_field

# Main function which facilitate the whole process of json flatten and multiple table segregation.
def main_json_flatten(src, ingress_config, rules_config):
    """
    to perform flatten transformation rule, flattens any json dataframe, also segregates to multiple tables in case of arrays present(on input from user)
    Parameters
    ----------
    src - source nested json dataframe
    ingress_config
    rules_config - json flatten rules section of ingress config
    ----------
    Calls appropriate functions to flatten the nested source and segregate the tables if any
    """
    basefilename = ingress_config["data"]["inputFile"]["fileName"].replace("/","").strip()
    process_name = f"DataTransformation-{basefilename}"
    
    # list of attributes with type struct or array
    nested_fields = list(dict([(field.name, field.dataType)  for field in src.schema.fields
                    if type(field.dataType) == ArrayType or type(field.dataType) == StructType]).keys())
    
    #attributes to be segregated to new tables
    split_tags = []
    split_tag_list = [a.strip() for a in list(rules_config['split_tags'].split("|")) if a != '']
    split_tags = [tag.split(":")[0].strip() for tag in split_tag_list] 
    # dropping split tags columns
    split_tags_fields = [b for b in nested_fields if b in split_tags]
    src_base_df = src.drop(*split_tags_fields)
    
    # ignore columns from base table
    ignored_fields = []
    ignore_tag_list = [c.strip() for c in list(rules_config['ignore_tags'].split("|")) if c != '']
    ignored_fields = msi_base_ignored_fields(ignored_fields, ignore_tag_list)
    # dropping ignored columns    
    src_base_df = src_base_df.drop(*ignored_fields)
    
    # CALLING THE FLATTEN FUNCTIONS FOR BASE TABLE
    src_base_df = msi_base_flatten_call(src_base_df)
    # call the default rename function
    src_base_df = utils.default_column_rename(src_base_df)
    # Add current timestamp
    src_base_df = src_base_df.withColumn("load_timestamp",current_timestamp()) 
    #scd merge check cols
    scd1_field = []
    scd1_tag_list = [g.strip() for g in list(rules_config['scd1_flag'].split("|")) if g != '']
    scd1_field = msi_base_scd1_list(scd1_field, scd1_tag_list)
        
    # write the base stream table to delta
    msi_base_scd1_normal_call(ingress_config, basefilename, process_name, src_base_df, scd1_field)
    
    def foreach_tag_flatten(srcdf, batchid):
        # looping to create table for split tags
        for tags in split_tags:
            #parent columns to be added to splitted table
            parent_cols = []
            parent_cols = msi_tag_parentcol_list(tags, parent_cols,split_tag_list)
                
            # drop the fields except the tag info and parent cols for that tag
            splittag_drop_fields = [n for n in srcdf.columns if n not in parent_cols and n != tags]
            src_split_df = srcdf.drop(*splittag_drop_fields)
            
            # CALLING THE FLATTEN FUNCTIONS FOR TAGS TABLE
            src_split_df = msi_tag_flatten_call(src_split_df)
            # call the default rename function    
            src_split_df = utils.default_column_rename(src_split_df)
            # Add current timestamp
            src_split_df = src_split_df.withColumn("load_timestamp",current_timestamp())
            
            # ignore columns from tags table
            drop_cols = []
            drop_cols = msi_tag_dropcol_list(tags, drop_cols,ignore_tag_list)
            # dropping ignored columns
            src_split_df = src_split_df.drop(*drop_cols)            
        
            # update the config target path for specific tag
            import copy
            ingress_config_tag = copy.deepcopy(ingress_config)
            
            ingress_config_tag["target"]["options"]["path"] = ingress_config_tag["target"]["options"]["path"].split(basefilename)[0] + basefilename + "_" + tags + "/"
            basefilename_tag = basefilename + "_" + tags
            
            #scd merge check cols
            scd1_tag_field = []
            scd1_tag_field = msi_tag_scd1_list(tags, scd1_tag_field,scd1_tag_list)
            
            # write the tag stream table to delta
            msi_tag_scd1_normal_call(tags, src_split_df, ingress_config_tag, basefilename_tag, scd1_tag_field)
    
    if not split_tags:
        print('Multiple table segregation is not applicable')
    else:
        if ingress_config["target"]["targetTrigger"] !='once':
            src.writeStream\
                .foreachBatch(foreach_tag_flatten)\
                .outputMode("append")\
                .option("checkpointLocation", ingress_config["target"]["options"]["checkpointLocation"].split(basefilename)[0] + basefilename + "_tags/")\
                .trigger(processingTime=ingress_config["target"]["targetTrigger"])\
                .start()
        else:
            src.writeStream\
                .foreachBatch(foreach_tag_flatten)\
                .outputMode("append")\
                .option("checkpointLocation", ingress_config["target"]["options"]["checkpointLocation"].split(basefilename)[0] + basefilename + "_tags/")\
                .trigger(once=True)\
                .start()\
                .awaitTermination()
