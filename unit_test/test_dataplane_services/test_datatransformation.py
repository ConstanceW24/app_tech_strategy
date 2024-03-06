from pyspark.sql import SparkSession
from dataplane_services import DataTransformation as dt
from unit_test.Initialize_pytests import expected_dataframe,test_result
import time
import logging

spark= SparkSession.builder.getOrCreate() 
def test_data_transformation(dt_config):
    """
    Tests whether data_transformation function applies transformation logic to sample data in streaming fashion

    This function is used to tests data_transformation function applies transformation logic to sample data in streaming fashion 

    Parameter:
        dt_config:json
    """
    try:
        expected_df=expected_dataframe(dt_config,'transformation')
    
        dt.data_transformation(dt_config)
        time.sleep(30)
        source_file_name = dt_config["data"]["inputFile"]["fileName"].replace("/","")
        output_df = spark.read.format("delta").table(f"{source_file_name}_merged")
        output_df=output_df.drop("rn","CDA_IsDeleted")
        assert str(output_df.schema) == str(expected_df.schema)
        test_result['test_data_transformation']='Pass' 
        print(f"Test case - test_data_transformation passed successfully")
    except AssertionError:
        test_result['test_data_transformation']='Fail' 
        logging.error(f"Test Case - test_data_transformation - Failed", exc_info=True)