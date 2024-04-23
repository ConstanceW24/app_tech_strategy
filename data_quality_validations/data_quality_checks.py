"""Data Quality
This script performs data quality checks on the source streaming data.

It receives the data quality config and source dataframe as parameters from the respective file e.g., data transformation etc,
iterates the rules list from the configuration and perform data quality check and handle the exception gracefuly
and returns the validated data back to the calling servic for next further processing/transformations.

This script requires to import the common_utils,ingress_utils,egress_utils files to refer the common functions inside the script.

This file can also be imported as a module and contains the following

functions:
    * dqperform - Performs DQ check and returns the spark dataframeback to the calling service.
"""


from pyspark.sql.functions import length, sum, to_date, to_timestamp, when, col, lower, lit, count, row_number, collect_set, size, regexp_replace
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import sys



spark= SparkSession.builder.getOrCreate()

#Perform Null Validation
def null_validation(src_df,rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(col(rule_dict['field_name']).isNull(),'Fail').when(col(rule_dict['field_name']) == 'Null','Fail').otherwise('Pass'))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df

#Perform Stringlength Validation
def stringlength_validation(src_df,rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(length(col(rule_dict['field_name'])) > rule_dict['validation_input'],'Fail').otherwise('Pass'))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df

#Perform Numeric Validation
def numeric_validation(src_df,rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(col(rule_dict['field_name']).cast('int').isNull(),'Fail').otherwise('Pass'))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df

#Perform Date Validation
def date_validation(src_df,rule_dict):
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(to_date(col(rule_dict['field_name']).cast("string"),rule_dict['validation_input']).isNull(),"Fail").otherwise("Pass"))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df

#Perform Alpha Numeric Validation
def alpha_numeric_validation(src_df,rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(col(rule_dict['field_name']).rlike("[^a-zA-Z0-9]"),"Fail").otherwise("Pass"))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df

#Perform Alpha Numeric Validation
def pattern_matching_validation(src_df,rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(regexp_replace(col(rule_dict['field_name']),rule_dict['validation_input'],"") != "","Fail").otherwise("Pass"))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df


#Perform SpecialCharacters Validation
def specialcharacter_validation(src_df,rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(col(rule_dict['field_name']).contains(rule_dict['validation_input']),'Pass').otherwise('Fail'))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df

#Perform Negative Validation
def negative_validation(src_df,rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(col(rule_dict['field_name']) < 0,'Fail').otherwise('Pass'))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df


#Perform Negative Validation
def range_validation(src_df,rule_dict):
    datatype, min_range, max_range = rule_dict['validation_input'].strip().split(',')
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(col(rule_dict['field_name']).cast(datatype.strip()).between(min_range.strip(), max_range.strip()),'Pass').otherwise('Fail'))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df


#Perform SpecificValue Validation
def specificvalue_validation(src_df,rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(col(rule_dict['field_name']).isin(rule_dict['validation_input'].split(',')),'Pass').otherwise('Fail'))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df

#Perform Decimal Validation
def decimal_validation(src_df,rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(col(rule_dict['field_name']).rlike("^\d*\.\d+$"),"Pass").otherwise("Fail"))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df

#Perform Timestamp Validation
def timestamp_validation(src_df,rule_dict):
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    res_df=src_df.withColumn(rule_dict['validation_output_field'],when(to_timestamp(col(rule_dict['field_name']).cast("string"),rule_dict['validation_input']).isNull(),"Fail").otherwise("Pass"))
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df


# Check whether the field value is unique or not
def unique_validation(src_df, rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'], when(size(collect_set(rule_dict['field_name']).over(Window.partitionBy(rule_dict['field_name']))) - count(rule_dict['field_name']).over(Window.partitionBy(rule_dict['field_name'])) == 0, "Pass").otherwise("Fail"))
    res_df=exceptionhandling(res_df,rule_dict) ## Won't retain any record with duplicates if exception_handling is Reject
    return res_df


# Check whether the field value is unique or not
def unique_validation_with_order_by(src_df, rule_dict):
    res_df=src_df.withColumn(rule_dict['validation_output_field'], when(row_number().over(Window.partitionBy(rule_dict['field_name']).orderBy(rule_dict['validation_input']))  > 1, "Fail").otherwise("Pass"))
    res_df=exceptionhandling(res_df,rule_dict) ## Will retain 1 record from duplicates
    return res_df



#Custom validation based on the user input
#sample for rule_override format "rule_override": "case when faxphonecountry='NULL' or faxphonecountry is null then 'Fail' else 'Pass' END as nullCustomValidation_faxphonecountry "

def custom_validation(src_df,rule_dict):
    import time
    timestamp_ts = time.time()
    timestamp_ms = int(timestamp_ts * 1000)

    rule=rule_dict['rule_override']
    begin_query="select * "
    begin_query+=','+rule
    table_name=f'dqrule_validation_{timestamp_ms}'
    final_query = begin_query +" from "+table_name #constructing a sql query
    try:
        src_df.createOrReplaceTempView(table_name)  # Creating a tempview from dataframe
        res_df = spark.sql(final_query) # Performing a DQ validation and store the results back to DF
    except Exception as e:
        print("Trying to run query on the batch df session")
        res_df = src_df._jdf.sparkSession().sql(final_query)
        res_df = DataFrame(res_df, spark)
    res_df=exceptionhandling(res_df,rule_dict)
    return res_df


def batch_count_check(df_batch, rule_dict, abort_exp):

    df = df_batch.filter(eval(abort_exp))
    failed_count = df.count()

    if failed_count > 0:
        sys.exit(f"DQ Check failed for {rule_dict['validation_name']} rule. Failed rows are {failed_count}. Aborting the process.")
    
    return df_batch


def stream_count_check(df_stream, rule_dict, abort_exp):

    from pyspark.sql.streaming import StreamingQueryException
    stream_df = df_stream.filter(eval(abort_exp)).agg(count("*").alias("record_count"))

    def count_check(df, batchid):
        record_count = df.collect()[0]["record_count"]
        if record_count > 0:
            #mail invoke
            raise StreamingQueryException(f"DQ Check failed for {rule_dict['validation_name']} rule. Failed rows are {record_count}. Aborting the process.")
        
    try:
        stream_df.writeStream.queryName("calculate counts")\
                            .foreachBatch(count_check)\
                            .outputMode("complete")\
                            .trigger(once=True)\
                            .start()\
                            .awaitTermination()
    except StreamingQueryException as e:
        print(f"Data Quality Streaming Process Failed with Exception: {e} ")


    return df_stream



#DQ Exception handling
def exceptionhandling(df_stream, rule_dict):
    #constructing a Exception handling condition for default
    default_value=f'''when(((col("{rule_dict['validation_output_field']}")=='Fail')&(lit("{rule_dict['exception_handling']}"=="Default"))) ,
       rule_dict['default_value']).otherwise(col("{rule_dict['field_name']}"))'''
    
    # constructing a Exception handling condition for reject
    reject_exp = f'''col("{rule_dict['validation_output_field']}")=="Pass"'''

    # constructing a Exception handling condition for Abort
    abort_exp = f'''col("{rule_dict['validation_output_field']}")=="Fail"'''

    if rule_dict.get('reject_handling','false').lower() == 'false':
        
        if rule_dict["exception_handling"].lower() == "default":
            df_stream = df_stream.withColumn(rule_dict['field_name'],eval(default_value))

        if rule_dict["exception_handling"].lower() == "reject":
            df_stream = df_stream.filter(eval(reject_exp))

        if rule_dict["exception_handling"].lower() == "ignore":
            return df_stream
        
        if rule_dict["exception_handling"].lower() == "abort":

            if rule_dict['batchFlag'].lower() == "true":
                return batch_count_check(df_stream, rule_dict, abort_exp)
            else:
                return stream_count_check(df_stream, rule_dict, abort_exp)
        
    return df_stream



def target_duplicate_validation(src_df,rule_dict):
    from pyspark.sql.functions import col, when, lit
    import json
    print("Validating Duplicate Check with Target")
    input_conf = json.loads(rule_dict['validation_input'])

    table_name = input_conf['table_name']
    tbl_nm = table_name.split(".")[-1]
    print("target_table:", tbl_nm)
    col_maps = input_conf['column_maps']

    if rule_dict['validation_output_field'] == None  or rule_dict['validation_output_field'] == '':
        rule_dict['validation_output_field'] = f"dqv_{tbl_nm}_duplicate"

    combined_cond = None
    first_tgt = ""
    for src_col, tgt_col in col_maps.items():
        cond = ( col(f"src.{src_col}") == col(f"tgt.{tgt_col}") )
        if combined_cond != None:
            combined_cond = combined_cond & cond
        else:
            first_tgt = tgt_col
            combined_cond = combined_cond


    try:
        target_df = spark.table(table_name)
        joined_df = src_df.alias("src").join(target_df.alias("tgt"), combined_cond, "left")
        case_condition = when(col(f"tgt.{first_tgt}").isNull(), "Pass").otherwise("Fail").alias(rule_dict['validation_output_field'])
        result_df = joined_df.select("src.*", case_condition)
    except Exception as e:
        result_df = src_df.withColumn(rule_dict['validation_output_field'],lit('Pass')) 
    
    res_df=exceptionhandling(result_df, rule_dict)  

    return res_df
