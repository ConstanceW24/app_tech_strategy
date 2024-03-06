
from common_utils import validateconfig as config
from unit_test.Initialize_pytests import test_result
import logging


def test_validate_basic_details(ingress_config): 
    """
    Tests whether function provides valid response for json with keys provided

    This function is used to tests required keys available in json captured by function and provides true response 

    Parameter:
        ingress_config:json
    """
    try:
        config_resp = config.validate_basic_details(ingress_config)
        assert config_resp == True
        test_result['test_validate_basic_details']='Pass'
        print(f"Test Case - test_validate_basic_details - passed successfully with values")
    except AssertionError:
        test_result['test_validate_basic_details']='Fail'
        logging.error(f"Test Case - test_validate_basic_details - Failed", exc_info=True)



def test_validate_source_keys_details(ingress_config): 
    """
    Tests whether function provides valid response for json with keys provided

    This function is used to tests required keys available in json captured by function and provides true response 

    Parameter:
        ingress_config:json
    """
    try:
        config_resp = config.validate_source_keys_details(ingress_config)
        assert config_resp == True
        test_result['test_validate_source_keys_details']='Pass'
        print(f"Test Case - test_validate_source_keys_details - passed successfully with values")
    except AssertionError:
        test_result['test_validate_source_keys_details']='Fail'
        logging.error(f"Test Case - test_validate_source_keys_details - Failed", exc_info=True)



def test_validate_source_param_details(ingress_config): 
    """
    Tests whether function provides valid response for json with keys provided

    This function is used to tests required keys available in json captured by function and provides true response 

    Parameter:
        ingress_config:json
    """
    try:
        config_resp = config.validate_source_param_details(ingress_config)
        assert config_resp == True
        test_result['test_validate_source_param_details']='Pass'
        print(f"Test Case - test_validate_source_param_details - passed successfully with values")
    except AssertionError:
        test_result['test_validate_source_param_details']='Fail'
        logging.error(f"Test Case - test_validate_source_param_details - Failed", exc_info=True)



def test_validate_target_details(ingress_config):
    """
    Tests whether function provides valid response for json with keys provided

    This function is used to tests required keys available in json captured by function and provides true response 

    Parameter:
        ingress_config:json
    """
    try:
        config_resp = config.validate_target_details(ingress_config)
        assert config_resp == True
        test_result['test_validate_target_details']='Pass'
        print(f"Test Case - test_validate_target_details - passed successfully with values")
    except AssertionError:
        test_result['test_validate_target_details']='Fail'
        logging.error(f"Test Case - test_validate_target_details - Failed", exc_info=True)