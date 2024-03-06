
from ingress_utils import s3_read as s3_read
from egress_utils import s3_write as s3_write
from unit_test.Initialize_pytests import test_result
import logging
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate() 

def test_s3_stream_write(ingress_config):
    """
    Tests writing s3 storage with required properties 

    This function is used to test writing s3 storage with required properties 

    Parameter:
        dt_config:json
    """
    try:
        input_df = s3_read.s3_read(ingress_config) 
        output_df = s3_write.s3_stream_write(input_df,ingress_config)
        expected="<class 'pyspark.sql.streaming.DataStreamWriter'>"
        assert expected == str(type(output_df))
        test_result['test_s3_stream_write']='Pass'
        print(f"Test case - test_s3_stream_write - passed successfully")
    except AssertionError:
        test_result['test_s3_stream_write']='Fail'
        logging.error(f"Test Case - test_s3_stream_write - Failed", exc_info=True)