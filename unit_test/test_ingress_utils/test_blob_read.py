from ingress_utils import blob_read as blob_read
from unit_test.Initialize_pytests import test_result
import logging
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate() 


def test_read_blobstream(dt_config): 
    """
    Tests reading blob storage in streaming fashion 

    This function is used to test reading blob storage in streaming fashion 

    Parameter:
        dt_config:json
    """
    try:
        source_file_name = dt_config["data"]["inputFile"]["fileName"].replace("/","")
        expected_df = spark.read.format("delta").table(f"{source_file_name}_raw")
        output_df = blob_read.read_blobstream(dt_config) 
        assert output_df.schema == expected_df.schema
        test_result['read_blobstream']='Pass'
        print(f"Test case - read_blobstream - passed successfully")
    except AssertionError:
        test_result['read_blobstream']='Fail'
        logging.error(f"Test Case - read_blobstream - Failed", exc_info=True)