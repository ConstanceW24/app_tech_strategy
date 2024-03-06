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


from pyspark.sql.functions import length, sum, to_date, to_timestamp, when, col, lower, lit, count, row_number, collect_set, size
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window



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

    rule=rule_dict['rule_override']
    begin_query="select * "
    begin_query+=','+rule
    table_name='Events'
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

#DQ Exception handling
def exceptionhandling(df_stream, rule_dict):
    #constructing a Exception handling condition for default
    default_value=f'''when(((col("{rule_dict['validation_output_field']}")=='Fail')&(lit("{rule_dict['exception_handling']}"=="Default"))) ,
       rule_dict['default_value']).otherwise(col("{rule_dict['field_name']}"))'''

    if rule_dict["exception_handling"] == "Default":
        df_stream = df_stream.withColumn(rule_dict['field_name'],eval(default_value))

    if rule_dict.get('reject_handling','true').lower() == 'true':
        # constructing a Exception handling condition for reject                

        reject_exp = f'''col("{rule_dict['validation_output_field']}")=="Pass"'''

        if rule_dict["exception_handling"] == "Reject":
            df_stream = df_stream.filter(eval(reject_exp))

    return df_stream