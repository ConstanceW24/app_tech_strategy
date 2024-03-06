from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys
import traceback
import argparse
from dataplane_services import batch_data_ingestion, data_ingestion, batch_data_quality, data_quality, batch_data_transformation, data_transformation, batch_data_persistence, data_persistence
from schema_utils import config_schema as schema
from uuid import uuid4
from configs import config_generator
from datetime import datetime

spark= SparkSession.builder.getOrCreate()



node_processes = {
                'DataPreProcessNode' : ['dpp','data_transformation', data_transformation.data_transformation, batch_data_transformation.batch_data_transformation],
                'DataIngestionNode' : ['di','data_ingestion',data_ingestion.data_ingestion, batch_data_ingestion.batch_data_ingestion],
                'DataQualityNode' : ['dq','data_quality', data_quality.data_quality, batch_data_quality.batch_data_quality],
                'DataTransformationNode' : ['dt','data_transformation', data_transformation.data_transformation, batch_data_transformation.batch_data_transformation],
                #'DataMaskingNodeNode' : ['dm','data_masking', data_masking.data_masking],
                #'DataDeAnonymizationNode' : ['da','data_de_anonymization', data_de_anonymization.data_de_anonymization],
                'DataPersistenceNode' : ['dp','data_persistence', data_persistence.data_persistence, batch_data_persistence.batch_data_persistence]
}



def get_arguments():
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')
    parser.add_argument('-a', '--app_config', dest='app_config', required = False, default="default",
                        help='application config')
    parser.add_argument('-e', '--environment', dest='env', required = False, default="demo",
                        help='application environment')
    parser.add_argument('-fsm', '--fsm_flag', dest='fsm_flag', required = False, action='store_true', 
                        help='pass flag for fsm dp run')
    parser.add_argument('-ers', '--ers_flag', dest='ers_flag', required = False, action='store_true', 
                        help='pass flag for ers dp run')
    parser.add_argument('-cip-header', '--cip-header', dest = 'cip_header', type=str, required= False, default="", help='The dictionary to read')

    parser.add_argument('-s', '--source-dtl', dest='source_dtl', type=str, required= True, help='{"source-type": "s3", "path":"path-to-source-file", "format":"cobol", "copybook" : "path-to-copybook"}')
    parser.add_argument('-t', '--target-dtl', dest='target_dtl', type=str, required= True, help='{"target-type":"delta", "path":"path-to-source-file", "format" : "xml"}')

    args = parser.parse_args()

    return args



def init_process():
    from configs.app_config import get_proj_configs, get_function_call, change_status, exception_marker, assign_run_metadata, get_init_list, get_node_list, get_partition_path, set_client_functions, get_config_filename, fix_dqrules, component_marker
    from configs import validateconfig as config
    from common_utils.batch_process_utils import check_truncate_load, get_old_target_counts, write_reconcile_count, archive_target, clear_spark_metadata, count_validation_check
    import ast
    try:
        args = get_arguments()

        cip_dict = {} if args.cip_header == "" else ast.literal_eval(args.cip_header)

        if cip_dict.get("journeyinstanceid","") != "":
            run_id = cip_dict.get("journeyinstanceid","")
        else:
            run_id = str(uuid4())

        if cip_dict.get("eventmapinstanceid","") != "":
            event_id = cip_dict.get("eventmapinstanceid","")
        else:
            event_id = ""

        project_config = get_proj_configs(args.app_config, args.env)
        client, processing_engine, get_content, get_list, clear_files, move_files, write_file, _ = set_client_functions(project_config)
        fsm_flag = args.fsm_flag
        ers_flag = args.ers_flag
        import json
        source_dict = json.loads(args.source_dtl)
        target_dict = json.loads(args.target_dtl)

        init_event_id  = ""

        init_event_id = (source_dict['path']).split('/')[-1]

        process_nm = node_processes['DataIngestionNode'][1]
        info_msg = [f'{process_nm.upper()} - STARTED']

        proc_config = config_generator.get_config(source_dict, target_dict, node="DataIngestionNode")
        proc_config['configuration']['processingEngine'] = processing_engine
        spark.conf.set("spark.sql.streaming.schemaInference","true")
        
        component_marker(fsm_flag, run_id, init_event_id, info_msg , "fsm", project_config, cip_dict)

        proc_config = assign_run_metadata(proc_config, process_nm, args.app_config, args.env, fsm_flag, ers_flag, run_id, event_id, cip_dict)

        ############# Validating Configuration ##################
        #config.valid_config_check(proc_config['configuration']) 

        ############# Calling Layer function ##################
        data_ingestion.data_ingestion(proc_config['configuration'])

        info_msg = change_status(info_msg, 'COMPLETED')
        component_marker(fsm_flag, run_id, init_event_id, info_msg, "fsm", project_config, cip_dict)


    except Exception as e:
        err_msg = f"Encountered exception during Initilization Process - {e}"
        print(err_msg)
        info_msg = change_status(info_msg,'FAILED')
        exception_marker(fsm_flag, ers_flag, run_id, event_id,  info_msg, err_msg, project_config, cip_dict)
        traceback.print_exc()
        sys.exit(1)

