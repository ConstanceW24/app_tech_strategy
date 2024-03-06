from ingress_utils import delta_read as delta_read
from unit_test.Initialize_pytests import test_result
import logging
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate() 


def test_delta_stream_read(dt_config): 
    """
    Tests reading delta table in streaming fashion 

    This function is used to test reading delta table in streaming fashion 

    Parameter:
        dt_config:json
    """
    try:
        source_file_name = dt_config["data"]["inputFile"]["fileName"].replace("/","")
        expected_df = spark.read.format("delta").table(f"{source_file_name}_raw")
        output_df = delta_read.delta_stream_read(dt_config) 
        assert output_df.schema == expected_df.schema
        test_result['test_delta_stream_read']='Pass'
        print(f"Test case - test_delta_stream_read - passed successfully")
    except AssertionError:
        test_result['test_delta_stream_read']='Fail'
        logging.error(f"Test Case - test_delta_stream_read - Failed", exc_info=True)