import os, sys, traceback
import time
import requests  
import json  
import argparse
from uuid import uuid4
from configs.app_config import get_proj_configs, set_client_functions

def trigger_job_run(url, headers, payload):
    response = requests.post(url, headers=headers, data=payload)  
    if response.status_code != 200:
        raise Exception(str(response.text))
    else:
        print(response.text)  

def walk_directories(project_config, directory, last_run_timestamp):
    client, processing_engine, get_content, get_list, clear_files, move_files, write_file = set_client_functions(project_config)

    files = get_list(client, directory, with_timestamp=True)
    tables_to_run = set()
    
    for file_path, tmsp in files:  
        if tmsp > last_run_timestamp:
            tables_to_run.add(file_path)
            print("File: {} , {}".format(file_path, tmsp))
    return tables_to_run



def wildcard_match(path, pattern):
    import fnmatch 
    return fnmatch.fnmatch(path, pattern)




def get_arguments():
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')
    parser.add_argument('-a', '--app_config', dest='app_config', required = False, default="default",
                        help='application config')
    parser.add_argument('-e', '--environment', dest='env', required = False, default="demo",
                        help='application environment')
  

    subparser = parser.add_subparsers(dest='proc_name', required = True)
    copy_conf  = subparser.add_parser('copy')
    move_conf  = subparser.add_parser('move')
    del_conf  = subparser.add_parser('delete')

    group_cp = copy_conf.add_mutually_exclusive_group()
    group_cp.add_argument('-s', '--source-path', dest='source_path')
    group_cp.add_argument('-t', '--target-path', dest='target_path')
    group_cp.add_argument('-r', '--recursive', dest='recursive', required = False, action='store_true')

    group_mv = move_conf.add_mutually_exclusive_group()
    group_mv.add_argument('-s', '--source-path', dest='source_path')
    group_mv.add_argument('-t', '--target-path', dest='target_path')
    group_mv.add_argument('-r', '--recursive', dest='recursive', required = False, action='store_true')

    group_del = del_conf.add_mutually_exclusive_group()
    group_del.add_argument('-t', '--target-path', dest='target_path')
    group_del.add_argument('-r', '--recursive', dest='recursive', required = False, action='store_true')
    
    args = parser.parse_args()

    return args



def util_process():
    
    try:
        args = get_arguments()
        project_config = get_proj_configs(args.app_config, args.env)
        client, processing_engine, get_content, get_list, clear_files, move_files, write_file, copy_files = set_client_functions(project_config)
        current_timestamp = time.time()
        if args.proc_name.lower() == 'copy':
            copy_files(client, args.source_path, args.target_path, args.recursive)
        elif args.proc_name.lower() == 'move':
            move_files(client, args.source_path, args.target_path, args.recursive)
        elif args.proc_name.lower() == 'delete':
            clear_files(client, args.target_path, args.recursive)
        
    except Exception as e:
        err_msg = f"Encountered exception during Initilization Process - {e}"
        print(err_msg)
        traceback.print_exc()
        sys.exit(1)

