from unit_test.Initialize_pytests import expected_dataframe,test_result,clean_up_tables
from unit_test.test_common_utils import test_validateconfig as validateconfig
from unit_test.test_dataplane_services import test_dataingestion as dataingestion
from unit_test.test_dataplane_services import test_dataquality as dataquality
from unit_test.test_dataplane_services import test_datatransformation as datatransformation
from unit_test.test_dataplane_services import test_datapersistence as datapersistence
from unit_test.test_ingress_utils import test_delta_read,test_s3_read,test_blob_read
from unit_test.test_egress_utils import test_blob_write,test_s3_write,test_delta_write 
from secrets_utils.secrets_manager import get_secrets
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate() 
import json
import requests
import base64
import argparse 
import traceback
import pandas

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils 

def execute_test():
    """
    Execute function used to fetch config and run all test functions 

    This function is used to get configid from job and fetch sample config from swagger,execute test function and return results

    Parameter:
        None

    Return
    ------
    return: dict
    """
    try:
         dbutils = get_dbutils(spark) 
         parser = argparse.ArgumentParser(description='unit test')
         parser.add_argument('--ingressconfigid', help='pass required ingressconfig parameters', nargs='+',required=True)
         parser.add_argument('--dataqualityid', help='pass required dataquality parameters', nargs='+',required=True) 
         parser.add_argument('--transformationconfigid', help='pass required transformationconfig parameters', nargs='+',required=True)
         parser.add_argument('--egressconfigid', help='pass required egressconfig parameters', nargs='+',required=True) 
         args = parser.parse_args()         
         ingressconfigid=args.ingressconfigid[0]
         print(ingressconfigid)
         dataqualityid=args.dataqualityid[0]
         transformationconfigid=args.transformationconfigid[0]
         egressconfigid=args.egressconfigid[0]
         
         config_url = get_secrets()['config_catalog_url']
         config_response = requests.get(config_url).text
         config=json.loads(config_response)
         config_df=pandas.DataFrame(config)
         test_config=config_df[config_df['configId'].str.contains(r'test(?!$)')]
         for config in test_config.iterrows():
           if config[1]['configId']==ingressconfigid:
            ingress_config=config[1]['configuration']
           elif config[1]['configId']==dataqualityid:
            dq_config=config[1]['configuration']
           elif config[1]['configId']==transformationconfigid:
            dt_config=config[1]['configuration']
           elif config[1]['configId']==egressconfigid:
            egress_config=config[1]['configuration']

         clean_up_tables(ingress_config,dq_config,egress_config)

         validateconfig.test_validate_basic_details(ingress_config)

         validateconfig.test_validate_source_keys_details(ingress_config)

         validateconfig.test_validate_target_details(ingress_config)

         validateconfig.test_validate_source_param_details(ingress_config) 

         dataingestion.test_data_ingestion(ingress_config)

         dataquality.test_data_quality(dq_config)

         datatransformation.test_data_transformation(dt_config)

         datapersistence.test_data_persistance(egress_config) 

         test_delta_read.test_delta_stream_read(dt_config) 

         test_delta_write.test_delta_stream_write(dt_config) 

         test_s3_read.test_s3_read(ingress_config) 

         test_s3_write.test_s3_stream_write(ingress_config) 

         if 'Fail' in test_result.values(): 
            test_result['Overall_result']='Fail' 
         else: 
             test_result['Overall_result']='Pass' 
         print("TEST RESULTS: ")
         for test in test_result.items():
              print(test)
         return dbutils.notebook.exit(json.dumps(test_result)) 
    except Exception as e:
        print(f"Encountered exception during Initilization Process - {e}")
        traceback.print_exc()


if __name__ == '__main__':
    execute_test()