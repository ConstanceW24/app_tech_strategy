
from pyspark.sql import SparkSession
from dataplane_services import DataIngestion as di
from unit_test.Initialize_pytests import expected_dataframe,test_result
import logging
import time

spark= SparkSession.builder.getOrCreate()  

def test_data_ingestion(ingress_config): 
    """
    Tests whether data_ingestion function ingests sample data

    This function is used to tests whether data_ingestion function ingests sample data provided in streaming fashion 

    Parameter:
        ingress_config:json
    """
    try:
        expected_df=expected_dataframe(ingress_config,'ingestion')
    
        di.data_ingestion(ingress_config)
        time.sleep(30)
        source_file_name = ingress_config["data"]["inputFile"]["fileName"].replace("/","")
        output_df = spark.read.format("delta").table(f"{source_file_name}_raw")
        cols=['cda_tablename','cda_fingerprint','cda_timestamp']
        output_df =output_df.drop(*cols)
        assert str(output_df.schema) == str(expected_df.schema)   
        assert output_df.count() == expected_df.count()
        test_result['test_data_ingestion']='Pass'
        print(f"Test case - test_data_ingestion - passed successfully")
    except AssertionError:
        test_result['test_data_ingestion']='Fail'
        logging.error(f"Test Case - test_data_ingestion - Failed", exc_info=True)