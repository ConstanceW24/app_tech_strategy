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
    client, processing_engine, get_content, get_list, clear_files, move_files, write_file, _ = set_client_functions(project_config)

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
    parser.add_argument('-c', '--config-file', dest='conf_file', required = True,
                        help='parent path for the configuration files')
    
    args = parser.parse_args()

    return args



def file_watcher_process():
    
    try:
        args = get_arguments()
        project_config = get_proj_configs(args.app_config, args.env)
        client, processing_engine, get_content, get_list, clear_files, move_files, write_file, _ = set_client_functions(project_config)

        current_timestamp = time.time()
        jobs_to_invoke = []
        config = get_content(client, args.conf_file, json_format=True)

        timestamp_file = config['timestamp_file']
        pattern_config_mapper = config['pattern_config_mapper']
        url = config['journey_url']
        headers = config['journey_headers']

        override = config['config_override']
        body = config['journey_payload']

        last_run_timestamp = 0  
        try:
            last_run_timestamp = float(get_content(client,timestamp_file))
        except Exception as e:
            print(e)
            pass

        print('Last Run Timestamp: ', last_run_timestamp) 

        for pattern, init_file in pattern_config_mapper.items():
            tables_to_run = []
            folder = "/".join(pattern.split('/')[:-1])
            tables_to_run = walk_directories(project_config, folder, last_run_timestamp)

            for path in tables_to_run:
                if wildcard_match(path, '*'+ pattern):
                    print(pattern, init_file)
                    print(path)
                    jobs_to_invoke += [init_file]

        for init_file in list(set(jobs_to_invoke)):
            data_payload = json.dumps(body).replace('<<init_file>>',init_file)
            headers = {**headers, **override.get(init_file,{})}
            print(data_payload)
            trigger_job_run(url, headers, data_payload)
            print(init_file)

        write_file(client, timestamp_file, str(current_timestamp), json_format=False, mode = 'overwrite')

    except Exception as e:
        err_msg = f"Encountered exception during Initilization Process - {e}"
        print(err_msg)
        traceback.print_exc()
        sys.exit(1)

