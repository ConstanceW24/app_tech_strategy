from dataplane_services import DataPersistence as dp
 
from pyspark.sql import SparkSession
import time
import datacompy
from pyspark.sql.functions import lower 
from unit_test.Initialize_pytests import test_result
import logging

spark= SparkSession.builder.getOrCreate()  

def test_data_persistance(egress_config):
    """
    Tests whether data_persistance function loads data to snowflake in streaming fashion

    This function is used to tests whether data_persistance function loads data to snowflake in streaming fashion 

    Parameter:
        egress_config:json
    """
    try:
        file_path=egress_config["source"]["driver"]["path"]
        expected=spark.sql(f"show columns in {file_path}_merged").toPandas()
        expected=expected.loc[expected["col_name"]!='rn']
        expected['col_name'] = expected['col_name'].str.lower()
        expected_count=spark.sql(f"select * from {file_path}_merged")
        
        dp.data_persistance(egress_config)
        time.sleep(50)
        sfoptions = dict(sfUrl=egress_config["target"]["options"]["sfUrl"],
                               sfUser=egress_config["target"]["options"]["sfUser"],
                               sfPassword=egress_config["target"]["options"]["sfPassword"],
                               sfDatabase=egress_config["target"]["options"]["sfDatabase"],
                               sfSchema=egress_config["target"]["options"]["sfSchema"],
                               sfWarehouse=egress_config["target"]["options"]["sfWarehouse"]) 
        column_query="select COLUMN_NAME from GW_DATAPIPELINE_POC.INFORMATION_SCHEMA.COLUMNS where table_name='{0}'  AND TABLE_SCHEMA='{1}';".format(f"{file_path}_merged".upper(),'GW_TARGET')
        output_df=spark.read.format("net.snowflake.spark.snowflake").options(**sfoptions).option("query",column_query).load()
        output=output_df.select(lower("COLUMN_NAME").alias("col_name")).toPandas()
        count_query="select * from GW_DATAPIPELINE_POC.GW_TARGET.{0};".format(f"{file_path}_merged".upper())
        output_count=spark.read.format("net.snowflake.spark.snowflake").options(**sfoptions).option("query",count_query).load()

        compare = datacompy.Compare(
              expected,
              output,
              join_columns = 'col_name',
              abs_tol = 0,
              rel_tol = 0, 
              df1_name = 'expected',
              df2_name = 'output' 
         )
        assert compare.matches() == True
        assert expected_count.count() == output_count.count() 
        
        test_result['test_data_persistance']='Pass'
        print(f"Test case - test_data_persistance - passed successfully")
    except AssertionError:
        test_result['test_data_persistance']='Fail'
        logging.error(f"Test Case - test_data_persistance - Failed", exc_info=True)
