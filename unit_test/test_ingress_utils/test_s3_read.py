from ingress_utils import s3_read as s3_read
from unit_test.Initialize_pytests import test_result
import logging
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate() 


def test_s3_read(ingress_config):
    """
    Tests reading s3 storage in streaming fashion 

    This function is used to test reading s3 storage in streaming fashion 

    Parameter:
        ingress_config:json
    """
    try:
        expected_df=spark.read.format("parquet").load(ingress_config["source"]["driver"]["path"])
        output_df = s3_read.s3_read(ingress_config)
        assert output_df.schema == expected_df.schema
        test_result['test_s3_read']='Pass'
        print(f"Test case - test_s3_read - passed successfully")
    except AssertionError:
        test_result['test_s3_read']='Fail'
        logging.error(f"Test Case - test_s3_read - Failed", exc_info=True)