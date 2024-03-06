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
from datetime import datetime

spark= SparkSession.builder.getOrCreate()



node_processes = {
                'DataPreProcessNode' : ['dpp','data_transformation', data_transformation.data_transformation, batch_data_transformation.batch_data_transformation],
                'DataIngestionNode' : ['di','data_ingestion',data_ingestion.data_ingestion, batch_data_ingestion.batch_data_ingestion],
                'DataQualityNode' : ['dq','data_quality', data_quality.data_quality, batch_data_quality.batch_data_quality],
                'DataTransformationNode' : ['dt','data_transformation', data_transformation.data_transformation, batch_data_transformation.batch_data_transformation],
                'DataPersistenceNode' : ['dp','data_persistence', data_persistence.data_persistence, batch_data_persistence.batch_data_persistence]
}



def get_arguments():
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')
    parser.add_argument('-a', '--app_config', dest='app_config', required = False, default="default",
                        help='application config')
    parser.add_argument('-e', '--environment', dest='env', required = False, default="demo",
                        help='application environment')
    parser.add_argument('-c', '--config-path', dest='conf_path', required = True,
                        help='parent path for the configuration files')
    parser.add_argument('-bm', '--bookmark_flag', dest='bookmark_flag', required = False, action='store_false', 
                        help='pass flag for bookmark and recover run')
    parser.add_argument('-fsm', '--fsm_flag', dest='fsm_flag', required = False, action='store_true', 
                        help='pass flag for fsm dp run')
    parser.add_argument('-ers', '--ers_flag', dest='ers_flag', required = False, action='store_true', 
                        help='pass flag for ers dp run')
    parser.add_argument('-clr', '--clear_flag', dest='clear_flag', required = False, action='store_true', 
                        help='Clear Metadata')
    parser.add_argument('-rid', '--run_id', dest='run_id', required = False, default=None, 
                        help='unique run id')
    parser.add_argument('-cip-header', '--cip-header', dest = 'cip_header', type=str, required= False, default="", help='The dictionary to read')


    subparser = parser.add_subparsers(dest='proc_name', required = True)
    init = subparser.add_parser('all')
    conf_di  = subparser.add_parser('di')
    conf_dt  = subparser.add_parser('dt')
    conf_dq  = subparser.add_parser('dq')
    conf_dm  = subparser.add_parser('dm')
    conf_da  = subparser.add_parser('da')
    conf_dp  = subparser.add_parser('dp')
    conf_dpp = subparser.add_parser('dpp')

    cf_descriptor = 'configuration file path'
    if_descriptor = 'init file path'
    id_descriptor = 'Configuration ID of the configuration files'

    init.add_argument('-if', '--init-file', dest='init_file',  nargs='+',required = True, help='init file path')

    group_di = conf_di.add_mutually_exclusive_group()
    group_di.add_argument('-id', '--conf-id', dest='conf_id',
                        help=id_descriptor)
    group_di.add_argument('-if', '--init-file', dest='init_file', nargs='+',
                        help=if_descriptor)

    group_dt = conf_dt.add_mutually_exclusive_group()
    group_dt.add_argument('-id', '--conf-id', dest='conf_id',
                        help=id_descriptor)
    group_dt.add_argument('-if', '--init-file', dest='init_file', nargs='+',
                        help=if_descriptor)

    group_dq = conf_dq.add_mutually_exclusive_group()
    group_dq.add_argument('-id', '--conf-id', dest='conf_id',
                        help=id_descriptor)
    group_dq.add_argument('-if', '--init-file', dest='init_file', nargs='+',
                        help=if_descriptor)
    
    group_dm = conf_dm.add_mutually_exclusive_group()
    group_dm.add_argument('-id', '--conf-id', dest='conf_id',
                        help=id_descriptor)
    group_dm.add_argument('-if', '--init-file', dest='init_file', nargs='+',
                        help=if_descriptor)
    
    group_da = conf_da.add_mutually_exclusive_group()
    group_da.add_argument('-id', '--conf-id', dest='conf_id',
                        help=id_descriptor)
    group_da.add_argument('-if', '--init-file', dest='init_file', nargs='+',
                        help=if_descriptor)

    group_dp = conf_dp.add_mutually_exclusive_group()
    group_dp.add_argument('-id', '--conf-id', dest='conf_id',
                        help=id_descriptor)
    group_dp.add_argument('-if', '--init-file', dest='init_file', nargs='+',
                        help=if_descriptor)

    group_dpp = conf_dpp.add_mutually_exclusive_group()
    group_dpp.add_argument('-id', '--conf-id', dest='conf_id',
                        help=id_descriptor)
    group_dpp.add_argument('-if', '--init-file', dest='init_file', nargs='+',
                        help=if_descriptor)

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
        elif args.run_id == None :
            run_id = str(uuid4())
        else:
            run_id = args.run_id

        if cip_dict.get("eventmapinstanceid","") != "":
            event_id = cip_dict.get("eventmapinstanceid","")
        else:
            event_id = ""

        project_config = get_proj_configs(args.app_config, args.env)
        client, processing_engine, get_content, get_list, clear_files, move_files, write_file, copy_files = set_client_functions(project_config)
        config_root = args.conf_path if args.conf_path[-1:] == '/' else args.conf_path + '/'
        config_path  = config_root + '{}/'
        reconcile_path = project_config.get("reconcile_path","")
        validate_path = project_config.get("validate_path","")
        log_path = project_config.get("log_path","")
        fsm_flag = args.fsm_flag
        ers_flag = args.ers_flag
        clear_metadata = args.clear_flag
        old_target_count = 0

        init_process_nm = "Data Initialization"
        init_event_id  = ""
        init_file_list = get_init_list(args)

        for init in init_file_list:
                       
            nodes_list = get_node_list(init, args)
            print(init)
            #info_msg = [f'{init_process_nm.upper()} {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - STARTED']
            info_msg = [f'{init_process_nm.upper()} - STARTED']
            init_event_id = nodes_list[0][3]
            retrace_flag_file = log_path + f"{run_id}_{init_event_id}_{init.split('/')[-1]}"

            try:
                if args.bookmark_flag :
                    last_bookmarked_step = get_content(client, retrace_flag_file, True)
                    skip_flag = False
                else :
                    print("Step Bookmarking - Disabled")
                    last_bookmarked_step = []
                    skip_flag = True
            except:
                last_bookmarked_step = []
                skip_flag = True
                pass

            print(last_bookmarked_step)

            component_marker(fsm_flag, run_id, init_event_id, info_msg , "fsm", project_config, cip_dict)

            for node in nodes_list:

                if last_bookmarked_step != [] and node[0] < last_bookmarked_step[0] :
                    print(f"{node} skipped")
                    continue

                if skip_flag != True and last_bookmarked_step != [] and node[0] == last_bookmarked_step[0] :
                    if node[2] == last_bookmarked_step[2] :
                        print(f"Retraced to Bookmark")
                        skip_flag = True
                    else:
                        print(f"{node} skipped")
                        continue
                
                process_nm = node_processes[node[1]][1]
                info_msg = [f'{process_nm.upper()} - STARTED']
                _, proc_config = get_config_filename(config_path.format(process_nm), node[2] , project_config)

                conf_table = proc_config["configuration"]["data"]["eventTypeId"]
                proc_config['configuration']['processingEngine'] = processing_engine
                spark.conf.set("spark.sql.streaming.schemaInference","true")
                proc_config = fix_dqrules(proc_config)
                
                component_marker(fsm_flag, run_id, conf_table, info_msg , "fsm", project_config, cip_dict)

                check_truncate_load(proc_config, project_config)
                
                ## Add partition value to target path
                proc_config['configuration']['source'] = get_partition_path(proc_config['configuration']['source'])
                proc_config['configuration']['target'] = get_partition_path(proc_config['configuration']['target'])

                function_call = get_function_call(proc_config, node, node_processes)

                proc_config = assign_run_metadata(proc_config, process_nm, args.app_config, args.env, fsm_flag, ers_flag, run_id, event_id, cip_dict)
                
                ## Count validation
                old_target_count = get_old_target_counts(proc_config, reconcile_path)

                ############# Validating Configuration ##################
                config.valid_config_check(proc_config['configuration']) 

                ############# Calling Layer function ##################
                function_call(proc_config['configuration'])
                
                write_reconcile_count(proc_config, reconcile_path, old_target_count)
                write_validate_count(proc_config, validate_path)

                ## Count validation
                if proc_config['configuration']['target'].get('validation',"false").lower() != 'false':
                    proc_config['configuration']['target']['reconcilePath'] = proc_config['configuration']['target'].get('reconcilePath',"") if proc_config['configuration']['target'].get('reconcilePath',"") != "" else reconcile_path
                    count_validation_check(proc_config['configuration'])
                
                archive_target(proc_config, project_config)

                if clear_metadata:
                    clear_spark_metadata(proc_config, project_config)

                info_msg = change_status(info_msg, 'COMPLETED')
                component_marker(fsm_flag, run_id, conf_table, info_msg, "fsm", project_config, cip_dict)

            ## print validation counts against all target tables  
            get_validate_summary(validate_path, run_id)
            
            info_msg = [f'{init_process_nm.upper()} {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - COMPLETED']
            component_marker(fsm_flag, run_id, init_event_id, info_msg , "fsm", project_config, cip_dict)

        if last_bookmarked_step != []:
            clear_files(client, retrace_flag_file, recursive = False)

    except Exception as e:
        err_msg = f"Encountered exception during Initilization Process - {e}"
        print(err_msg)
        info_msg = change_status(info_msg,'FAILED')
        write_file(client, retrace_flag_file, node, True)
        exception_marker(fsm_flag, ers_flag, run_id, event_id,  info_msg, err_msg, project_config, cip_dict)
        traceback.print_exc()
        sys.exit(1)

