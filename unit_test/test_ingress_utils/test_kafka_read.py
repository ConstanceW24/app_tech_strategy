from ingress_utils import confluent_kafka_read as confluent_kafka_read
from unit_test.Initialize_pytests import test_result
import logging
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate() 
import pytest


def test_read_kafkastream(response_value,ingress_config):
    """	
    get the ingress config file and response value then call read_kafkastream function,
    test results with pytest. 
    
	Parameters
	----------
	response_value: string
       actual value which needs will be compared with results from function to test.
    ingress_config:json
	    The ingress configuration
	
	Return
	------
	None
	
	"""
    try:
        raw_df2 = confluent_kafka_read.read_kafkastream(ingress_config)
        assert raw_df2.schema == response_value
        print(raw_df2.schema)
    except Exception:
        with pytest.raises(Exception) as e_info:
             raw_df2 = confluent_kafka_read.read_kafkastream()
        assert str(type(e_info.value)) == response_value
        print(type(e_info.value))