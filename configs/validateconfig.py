# Databricks notebook source
from pyspark.sql import SparkSession
import sys
import json
import hashlib
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, count, datediff, udf, when, explode, expr, lit, regexp_replace, \
    split,array,substring,trim,to_date
from pyspark.sql.types import IntegerType, StringType,DateType
import logging
import boto3
import requests
import traceback





def validate_basic_details(ingress_config):
    """
    This function gets the data config and validates all the basic details are part of configuration
    and returns whether the configuration is valid or invalid.
    ----------
    ingress_config

    Return
    ------
    Boolean

    """
    ########### Validate the Basic Details ##############
    basic_keys = ["status","rules","source","target","data"]
    for k in basic_keys:
        if k in ingress_config.keys():
            if ingress_config[k] is None or ingress_config[k] == "":
                print(f"The value for {k} is blank")
                return False
            if k == "status" and ingress_config[k] == "Inactive":
                print("This config is Inactive")
                return False
        else:
            print(f"{k} is not present in Config file")
            return False
    print("Basic Keys Validation Complete")
    return True


def validate_source_keys_details(ingress_config):
    """
    This function gets the data config and validates all the source key details are part of configuration
    and returns whether the configuration is valid or invalid.
    ----------
    ingress_config

    Return
    ------
    Boolean

    """
    ########### Validate the Source Details ##############
    source_keys = ["driver","dependents"]
    for k in source_keys:
        if k in ingress_config["source"]:
            if ingress_config["source"][k] is None or ingress_config["source"][k] == "":
                print(f"The value for {k} in source is blank")
                return False
        else:
            print(f"{k} is not present in Source")
            return False
    print("Source Keys Validation Complete")
    return True


def validate_source_param_details(ingress_config):
    """
    This function gets the data config and validates all the source param details are part of configuration
    and returns whether the configuration is valid or invalid.
    ----------
    ingress_config

    Return
    ------
    Boolean

    """
    source_param_keys = ["inputFileFormat","options"]
    if "inputFile" not in ingress_config["data"]:
            return False
    for k in source_param_keys:
        if k in ingress_config["data"]["inputFile"]:
            if ingress_config["data"]["inputFile"][k] is None or ingress_config["data"]["inputFile"][k] == "":
                print(f"The value for {k} in data or Input file params is blank")
                return False
        else:
            print(f"{k} is not present in data or Input file params")
            return False
    print("Data Keys Validation Complete")
    return True

def validate_target_details(ingress_config):
    """
    This function gets the data config and validates all the target details are part of configuration
    and returns whether the configuration is valid or invalid.
    ----------
    ingress_config

    Return
    ------
    Boolean

    """
    ########### Validate the Target Details ##############
    target_keys = ["targetType","options"]
    for k in target_keys:
        if k in ingress_config["target"]:
            if ingress_config["target"][k] is None or ingress_config["target"][k] == "":
                print(f"The value for {k} in target is blank")
                return False
        else:
            print(f"{k} is not present in target")
            return False
    print("Target Keys Validation complete")
    return True


def parse_and_validate_config(ingress_config):
    """
    This function invokes all the validations and raise the exception if any of the validations fails.
    and returns whether the configuration is valid or invalid.
    ----------
    ingress_config

    Return
    ------
    Boolean

    """
    try:
        res1=validate_basic_details(ingress_config)
    except Exception as e:
        print(f"Encountered exception while running the job  - {e}")
        traceback.print_exc()

    try:
        res2=validate_source_keys_details(ingress_config)
    except Exception as e:
        print(f"Encountered exception while running the job  - {e}")
        traceback.print_exc()

    try:
        res3=validate_source_param_details(ingress_config)
    except Exception as e:
        print(f"Encountered exception while running the job  - {e}")
        traceback.print_exc()
    try:
        res = validate_target_details(ingress_config)
    except Exception as e:
        print(f"Encountered exception while running the job  - {e}")
        traceback.print_exc()

    if res1 and res2 and res3 and res:
        return True
    else:
        return False


def valid_config_check(ingress_config):
    """
    check for valid ingress config format

    Parameters
    ----------
    ingress_config

    Output
    ------
    Exists in case on invalid inout config else pass

    """

    validate_config = parse_and_validate_config(ingress_config)
    if validate_config:
        print("Valid Config")
    else:
        raise TypeError("Invalid Config")
