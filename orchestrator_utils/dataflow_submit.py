from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys
import traceback
import argparse
from dataplane_services import batch_data_ingestion, data_ingestion, batch_data_quality, data_quality, batch_data_transformation, data_transformation, batch_data_persistence, data_persistence
from schema_utils import config_schema as schema
from uuid import uuid4
from common_utils.batch_process_utils import get_target_counts, write_df_to_path, get_validate_summary, write_validate_count
from batch_audit import appl_audit_utils
from datetime import datetime

spark= SparkSession.builder.getOrCreate()



node_processes = {
                'DATA_PREPROCESS' : ['dpp','data_transformation', data_transformation.data_transformation, batch_data_transformation.batch_data_transformation],
                'DATA_INGESTION' : ['di','data_ingestion',data_ingestion.data_ingestion, batch_data_ingestion.batch_data_ingestion],
                'DATA_QUALITY' : ['dq','data_quality', data_quality.data_quality, batch_data_quality.batch_data_quality],
                'DATA_TRANSFORMATION' : ['dt','data_transformation', data_transformation.data_transformation, batch_data_transformation.batch_data_transformation],
                'DATA_PERSISTENCE' : ['dp','data_persistence', data_persistence.data_persistence, batch_data_persistence.batch_data_persistence]
}



def get_arguments():
    import argparse
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')

    parser.add_argument('-a', '--appl_short_name', dest='appl_short_name', required = True, help='pass Application short name')
    parser.add_argument('-src', '--source', dest='source', required = True, help='pass source for run')
    parser.add_argument('-db','--schema', dest='schema', required = False, default='edw_shared', help='pass schema for dp run')
    parser.add_argument('-tbl','--table', dest='table', required = False, default = None , help='pass schema for dp run')
    parser.add_argument('-sq','--sequence', dest='sequence', required = False, default = None , help='pass sequence for dp run')
    parser.add_argument('-ly','--layer', dest='layer', required = False, default = None , help='pass sequence for dp run')
    parser.add_argument('-e', '--environment', dest='env', required = True, default="dev", help='application environment')
    args = parser.parse_args()

    return args



def init_process():
    from configs.app_config import get_proj_configs, get_function_call,  set_client_functions
    from configs import validateconfig as config
    from common_utils.batch_process_utils import  get_old_target_counts, write_reconcile_count, archive_target, count_validation_check
    import ast
    task_id = ""
    try:
        args = get_arguments()
        project_config = get_proj_configs(args.env)
        client, processing_engine, get_content, get_list, clear_files, move_files, write_file, copy_files = set_client_functions(project_config)
        reconcile_path = project_config.get("reconcile_path","")
        validate_path = project_config.get("validate_path","")
        log_path = project_config.get("log_path","")
        
        task_df = appl_audit_utils.get_task_details(appl_short_name = args.appl_short_name , source = args.source, schema = args.schema, table_name = args.table, load_sequence_nbr = args.sequence, layer = args.layer)

        for rec in task_df.collect():

            task_id = ''

            proc_config = appl_audit_utils.get_task_config(rec)
            
            task_id = proc_config["configuration"]["run_id"]
            batch_id = proc_config["configuration"]["batch_id"]
            table_name = proc_config["configuration"]["table_name"]
            node = proc_config["configuration"]["data_layer"]

            appl_audit_utils.update_task_status(task_id, status = 'IN-PROGRESS', schema = args.schema)

            proc_config['configuration']['processingEngine'] = processing_engine

            function_call = get_function_call(proc_config, ['',node.upper()], node_processes)

            
            ## Count validation
            old_target_count = get_old_target_counts(proc_config, reconcile_path)

            ############# Validating Configuration ##################
            config.valid_config_check(proc_config['configuration']) 

            ############# Calling Layer function ##################
            function_call(proc_config['configuration'])
                
            ## Count validation
            if proc_config['configuration']['target'].get('validation',"false").lower() != 'false':
                write_reconcile_count(proc_config, reconcile_path, old_target_count)
                write_validate_count(proc_config, validate_path)
                proc_config['configuration']['target']['reconcilePath'] = proc_config['configuration']['target'].get('reconcilePath',"") if proc_config['configuration']['target'].get('reconcilePath',"") != "" else reconcile_path
                count_validation_check(proc_config['configuration'])
                # print validation counts against all target tables  
                get_validate_summary(validate_path, task_id)
            
            archive_target(proc_config, project_config)
            
            appl_audit_utils.update_task_status(task_id, status = 'COMPLETED', schema = args.schema)

    except Exception as e:
        err_msg = f"Encountered exception during Initilization Process - {e}"
        print(e)
        traceback.print_exc()
        appl_audit_utils.update_task_status(task_id, status = 'FAILED', schema = args.schema, message = str(e))
        sys.exit(1)

