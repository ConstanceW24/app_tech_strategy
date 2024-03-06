from ingress_utils import blob_read as blob_read
from egress_utils import blob_write as blob_write
from unit_test.Initialize_pytests import test_result
import logging
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate() 


def test_blob_stream_write(ingress_config):
    """
    Tests writing blob storage with required properties 

    This function is used to test writing blob storage with required properties 

    Parameter:
        ingress_config:json
    """
    try:
        spark.sql("set spark.sql.streaming.schemaInference=true")
        input_df = blob_read.read_blobstream(ingress_config)
        output_df = blob_write.blob_stream_write(input_df,ingress_config)
        expected="<class 'pyspark.sql.streaming.DataStreamWriter'>"
        assert expected == str(type(output_df))
        test_result['test_blob_stream_write']='Pass'
        print(f"Test case - test_blob_stream_write - passed successfully")
    except AssertionError:
        test_result['test_blob_stream_write']='Fail'
        logging.error(f"Test Case - test_blob_stream_write - Failed", exc_info=True)