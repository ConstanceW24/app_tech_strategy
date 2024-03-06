from egress_utils import kafka_write as kafka_write
from ingress_utils import confluent_kafka_read as confluent_kafka_read, s3_read as s3_read
from unit_test.Initialize_pytests import test_result
import logging
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,to_json,struct
spark= SparkSession.builder.getOrCreate() 

def test_kafka_stream_write(ingress_config1,ingress_config2):
    """	
    get two type of ingress config file -one to write into kafkastream and other to read kakfastream and compare results.
    
	Parameters
	----------
	ingress_config1: json
       The ingress configuration to write into kafkastream
    ingress_config2:json
	    The ingress configuration to read kafkastream
	
	Return
	------
	None
	
	"""
    try:
        raw_df = s3_read.s3_read(ingress_config1)
        src_df=raw_df.select(to_json(struct("*")).alias("value"))
        kafka_write.kafka_stream_write(src_df, ingress_config1)
        post_df = confluent_kafka_read.read_kafkastream(ingress_config2)
        post_df = post_df.selectExpr("CAST(value as STRING)")
        post_df = post_df.select(from_json(post_df.value, ingress_config1["data"]["inputFile"]["schema"]).alias("data"))
        post_df = post_df.select("data.*")
        assert raw_df.schema == post_df.schema
        print(post_df.schema)
    except Exception :
        with pytest.raises(Exception) as e_info:
            kafka_write.kafka_stream_write(src_df, ingress_config1)
            post_df = confluent_kafka_read.read_kafkastream(ingress_config2)
        response_value="<class 'pyspark.sql.utils.IllegalArgumentException'>"
        assert str(type(e_info.value)) == response_value
        print(type(e_info.value))