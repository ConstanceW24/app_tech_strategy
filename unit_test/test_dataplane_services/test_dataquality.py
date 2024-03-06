from pyspark.sql import SparkSession
from pyspark.sql.functions import (array, coalesce, col, count, current_date,
                                   datediff, explode, expr, length, lit, lower,
                                   regexp_replace, split, substring, sum,
                                   to_date, to_timestamp, trim, udf, when,md5,concat_ws)
from dataplane_services import DataQuality as dq
import logging
import datacompy
import time
from unit_test.Initialize_pytests import test_result 

spark= SparkSession.builder.getOrCreate() 

def test_data_quality(dq_config):
    """
    Tests whether data_quality function apply data quality checks to provided data

    This function is used to tests whether data_quality function apply data quality checks to provided data in streaming fashion 

    Parameter:
        dq_config:json
    """
    try:
        expected=spark.read.parquet(dq_config['source']['driver']['path'])
        for i in dq_config['rules']:
            null_validation=f'''when((col("{i['field_name']}").isNull()) | (col("{i['field_name']}")=='NULL') , "Fail").otherwise("Pass")'''
            time_validation=f'''when(to_timestamp(col("{i['field_name']}").cast("string"),"{i['validation_input']}").isNull(), "Fail").otherwise("Pass")'''
            numeric_validation=f'''when(col("{i['field_name']}").cast("int").isNull(), "Fail").otherwise("Pass")'''
            date_validation=f'''when(to_date(col("{i['field_name']}").cast("string"),"{i['validation_input']}").isNull(), "Fail").otherwise("Pass")'''
            alpha_numeric_validation=f'''when(col("{i['field_name']}").rlike("[^a-zA-Z0-9]"), "Fail").otherwise("Pass")'''
            specialcharacter_validation=f'''when(col("{i['field_name']}").contains("{i['validation_input']}"), "Pass").otherwise("Fail")'''
            stringlength_validation=f'''when(length(col("{i['field_name']}"))>"{i['validation_input']}", "Fail").otherwise("Pass")'''
            negative_validation=f'''when(col("{i['field_name']}")<0, "Fail").otherwise("Pass")'''
            specificvalue_validation=f'''when(col("{i['field_name']}").isin({i['validation_input']}),"Pass").otherwise("Fail")'''
            decimal_validation=f'''when(col("{i['field_name']}").rlike("[/^\d*\.\d*$/]"), "Fail").otherwise("Pass")'''               
            if(i['rule_parser'].lower()=="pyspark"):  
                validation=eval(i['validation_name'])
                expected=expected.withColumn(i['validation_output_field'],eval(validation))
        dq.data_quality(dq_config)
        time.sleep(30)
        output=spark.read.format("delta").load(dq_config['target']['options']['path'])
        assert str(expected.columns) == str(output.columns)
        assert expected.count() == output.count() 
        columns=[]
        for cols in output.columns:
            columns.append(cols)
        expected=expected.withColumn("md5",md5(concat_ws("",*columns))).select("md5").toPandas()
        output=output.withColumn("md5",md5(concat_ws("",*columns))).select("md5").toPandas()
        compare = datacompy.Compare(
              expected,
              output,
              join_columns = 'md5',
              abs_tol = 0,
              rel_tol = 0, 
              df1_name = 'expected',
              df2_name = 'output' 
         )
        assert compare.matches() == True
        test_result['test_data_quality']='Pass'
        print(f"Test case - test_data_quality - passed successfully")
    except AssertionError:
        test_result['test_data_quality']='Fail'
        logging.error(f"Test Case - test_data_quality - Failed", exc_info=True)