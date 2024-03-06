
from ingress_utils import delta_read as delta_read
from egress_utils import delta_write as delta_write
from unit_test.Initialize_pytests import test_result
import logging
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate() 


def test_delta_stream_write(dt_config):
    """
    Tests writing delta table with required properties 

    This function is used to test writing delta table with required properties 

    Parameter:
        dt_config:json
    """
    try:
        input_df = delta_read.delta_stream_read(dt_config) 
        output_df = delta_write.delta_stream_write(input_df, dt_config)
        expected="<class 'pyspark.sql.streaming.DataStreamWriter'>"
        assert expected == str(type(output_df))
        test_result['test_delta_stream_write']='Pass'
        print(f"Test case - test_delta_stream_write - passed successfully")
    except AssertionError:
        test_result['test_delta_stream_write']='Fail'
        logging.error(f"Test Case - test_delta_stream_write - Failed", exc_info=True)