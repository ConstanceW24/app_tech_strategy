# Databricks notebook source
"""Data Schema
This script defines the schema for Data initialization and Data config. 
Data Initialization service refer these schema and parse the json configuration and passing the configuration 
as paramter to all the data plane services.
 
"""

from pyspark.sql import SparkSession 
from pyspark.sql.types import StringType, StructType,StructField,ArrayType,MapType

spark= SparkSession.builder.getOrCreate()

init_schema = StructType([ 
    StructField("id",StringType(),True), 
    StructField("initializationId",StringType(),True),
    StructField("name",StringType(),True),
    StructField("description",StringType(),True),
    StructField("initializationType",StringType(),True),
    StructField("nodes", ArrayType(
                                   StructType([
                                        StructField("nodeId",StringType()),
                                        StructField("nodeName",StringType()),
                                        StructField("description",StringType()),
                                        StructField("eventTypeId",StringType()),
                                        StructField("eventTypeName",StringType()),
                                        StructField("configuration", MapType(StringType(),StringType()))
                                   ])
                                   )),
    StructField("edges", ArrayType(MapType(StringType(),StringType()))),
    StructField("eventTypeId", StringType(), True),
    StructField("version", StringType(), True),
    StructField("status", StringType(), True),
    StructField("createdBy", StringType(), True), 
    StructField("createdDate", StringType(), True),
    StructField("updatedBy", StringType(), True),
    StructField("updatedDate", StringType(), True)])

config_schema = StructType([
    StructField("id",StringType(),True),
    StructField("configId",StringType(),True),
    StructField("name",StringType(),True),
    StructField("description",StringType(),True),
    StructField("version",StringType(),True),
    StructField("configType",StringType(),True),
    StructField("status",StringType(),True),
    StructField("configuration",StructType([
                                    StructField("processingEngine",StringType(),True),
                                    StructField("version",StringType(),True),
                                    StructField("status",StringType(),True),
                                    StructField("source",StructType([
                                                               StructField("driver",MapType(StringType(),StringType())),
                                                               StructField("dependents",ArrayType(MapType(StringType(),StringType()))),
                                                               StructField("partitionKey",StringType(),True),
                                                               StructField("partitionDefault",StringType(),True),
                                                               StructField("primary_keys",ArrayType(StringType())),
                                                               StructField("reject_handling",StringType(),True),
                                                               StructField("column_mapping",StringType(),True),
                                                               StructField("datatype_mapping",ArrayType(MapType(StringType(),StringType())))
                                                                 ])),
                                    StructField("target",StructType([
                                                             StructField("targetType",StringType(),True),
                                                             StructField("noofpartition",StringType(),True),
                                                             StructField("targetTrigger",StringType(),True),
                                                             StructField("fixHeader",StringType(),True),
                                                             StructField("truncateLoad",StringType(),True),
                                                             StructField("mergeQueryFlag",StringType(),True),
                                                             StructField("options",MapType(StringType(),StringType())),
                                                             StructField("rejectOptions",MapType(StringType(),StringType()))
                                                                ])),
                                    StructField("rules",ArrayType(MapType(StringType(),StringType()))),
                                    StructField("dqrules",StructType([
                                                             StructField("rules",ArrayType(MapType(StringType(),StringType()))),
                                                             StructField("postdtchecks",StringType(),True)
                                                                ])),
                                    StructField("miscellaneous",ArrayType(StringType())),
                                    StructField("addColumns",MapType(StringType(),StringType())),
                                    StructField("data",StructType([
                                                            StructField("inputFile",StructType([
                                                                                          StructField("fileName",StringType()),
                                                                                          StructField("inputFileFormat",StringType()),
                                                                                          StructField("schema",StringType()),
                                                                                          StructField("primary_key",ArrayType(StringType())),
                                                                                          StructField("batchFlag", StringType()),
                                                                                          StructField("options",MapType(StringType(),StringType()))])),  
                                                            StructField("eventTypeId",StringType(),True)
                                                              ]))
                                ]) ,True),
    StructField("createdDate",StringType(),True),
    StructField("createdBy",StringType(),True),
    StructField("updatedDate",StringType(),True),
    StructField("updatedBy",StringType(),True),
    ])