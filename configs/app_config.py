from datetime import datetime
from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate()
from uuid import uuid4

project_config = {
"default" :{
"dev" : {
"environment" :"dev",
"platform" : "azure",                              # aws , gcp
"processing_engine" : "databricks",                # emr, databricks
"reconcile_path":"/mnt/mkl-dds-fsys-edl-dev-001/validation/",   # Path required to enable data validation
"log_path" : "/mnt/mkl-dds-fsys-edl-dev-001/logs/",
}
}
}


node_map = {
    'dpp': 'DataPreProcessNode',
    'di' : 'DataIngestionNode',
    'dq' : 'DataQualityNode',
    'dt' : 'DataTransformationNode',
    'dm' : 'DataMaskingNodeNode',
    'da' : 'DataDeAnonymizationNode',
    'dp' : 'DataPersistenceNode'
}


def get_proj_configs(app="default", env = "demo"):
    import json
    # Get configuration details
    if app not in project_config.keys():
        app = "default"

    return json.loads(json.dumps(project_config[app]).replace("<<env>>",env.strip()))


def get_function_call(proc_config, node, node_processes):
    if str(proc_config['configuration']["data"]["inputFile"].get('batchFlag','none')).lower() == 'true':
        function_call =  node_processes[node[1]][3]
    else:
        function_call =  node_processes[node[1]][2]
    return function_call



def change_status(info_msg, status):
    import re
    info_msg[0] = re.sub(r'[0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}', datetime.now().strftime('%Y-%m-%d %H:%M:%S') , info_msg[0].replace('STARTED', status))
    return info_msg



def get_init_list(args):
    config_root = args.conf_path if args.conf_path[-1:] == '/' else args.conf_path + '/'
    config_path  = config_root + '{}/'
    # Listing Init files
    if args.proc_name == 'all' or ( args.init_file != [None] and args.init_file != None):
        init_file_list = [ config_path.format('data_initialization') + i for i in args.init_file if i ]
    else:
        init_file_list = [None]
    
    return init_file_list


def get_node_list(init, args):
    from orchestrator_utils.data_init_generic import  node_processes
    project_config = get_proj_configs(app = args.app_config, env = args.env)
    nodes_list = []
    if init:
        event_type_id = get_json_attributes(init, 'eventTypeId', project_config)
        nodes_list = get_json_attributes(init, 'nodes', project_config)
        nodes_list = [ (node['nodeId'],
                        'DataPersistenceNode' if node['nodeName'] == 'DataPersistance' else node['nodeName'], 
                        node['configuration']['configId'],
                        event_type_id) 
                        for node in nodes_list if args.proc_name == node_processes['DataPersistenceNode' if node['nodeName'] == 'DataPersistance' else node['nodeName']][0] or args.proc_name == 'all' ]
    elif args.conf_id:
        nodes_list = [ ("101",node_map[args.proc_name], args.conf_id, args.conf_id[:-6])]

    return nodes_list


def fix_dqrules(proc_config):
    if 'configuration' in proc_config.keys() and 'dqrules' in proc_config['configuration'].keys():
        if 'list' in str(type(proc_config['configuration']['dqrules'])):
            proc_config['configuration']['dqrules'] = {'rules' : proc_config['configuration']['dqrules'], 'postdtchecks':'false'}
    return proc_config


def assign_run_metadata(proc_config, node_processes, app_conf, env,  runinstanceid):
    from uuid import uuid4
    table = '_'.join(proc_config['configId'].split('_')[:-1])
    app_id = uuid4().hex
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    proc_config['configuration']['tableName'] = table
    proc_config['configuration']['runTime'] = runtime
    proc_config['configuration']['appId'] = app_id
    proc_config['configuration']['app_conf'] = app_conf
    proc_config['configuration']['env'] = env
    proc_config['configuration']['nodeProcess'] = node_processes

    return proc_config


def get_partition_path(config):
    try:
        if config.get('partitionKey', '') != "":
            partition_key = config['partitionKey']
            if config.get('partitionDefault', '') != "":
                partition_default_logic = config['partitionDefault']  # datetime.strftime(datetime.now(), '%Y%m')
                partition_value = str(eval(partition_default_logic))
                if config.get("driver", {}) != {} and config['driver'].get('path', '') != '':
                    config['driver']['path']  += f"{partition_key}={partition_value}/"
                elif config.get("options", {}) != {} and config['options'].get('path', '') != '':
                    config['options']['path']  += f"{partition_key}={partition_value}/"
        return config
    except Exception as e:
        raise ValueError(f"Error in get_partition_path:{e}")
    

def set_client_functions(project_config):
    
    if project_config.get("processing_engine", '').lower() == 'databricks' :
        from common_utils.utils import get_dbutils
        from common_utils.utils import get_databricks_content as get_content, get_databricks_list as get_list, clear_databricks_files as clear_files, databricks_move_files as move_files, write_databricks_file as write_file, databricks_copy_files as copy_files
        client = get_dbutils(spark)
    elif(project_config.get("platform", '').lower() == 'aws'):
        import boto3
        from common_utils.utils import get_s3_content as get_content, get_s3_list as get_list, clear_s3_files as clear_files, s3_move_files as move_files, write_s3_file as write_file, s3_copy_files as copy_files
        client = boto3.client('s3')
    else:
        # below imports are just place holders , it supports databricks azure at the moment
        from common_utils.utils import get_databricks_content as get_content, get_databricks_list as get_list, clear_databricks_files as clear_files, databricks_move_files as move_files, write_databricks_file as write_file, databricks_copy_files as copy_files
        from common_utils.utils import get_dbutils
        client = get_dbutils(spark)
    
    processing_engine = project_config.get("processing_engine", 'databricks').title()
    return client, processing_engine, get_content, get_list, clear_files, move_files, write_file, copy_files

def get_json_attributes(file, attribute, project_config):
    client, processing_engine, get_content, get_list, clear_files, move_files, write_file, _ = set_client_functions(project_config)
    return get_content(client, file, json_format = True)[attribute]

def get_config_filename(path, config_id, project_config):
    client, processing_engine, get_content, get_list, clear_files, move_files, write_file, _ = set_client_functions(project_config)
    for file in get_list(client, path):
        proc_config = get_content(client, file, json_format = True)
        if proc_config['configId'] == config_id:
            return file, proc_config
    return None, None





#demo code added by yasir jan24  --begin
import time
import json
import os
from common_utils import utils as utils


def add_dependent_tasks(task_list):
    for current_task in task_list:
        # Initialize the dependent_tasks list for the current task
        current_task['dependent_tasks'] = []
        current_node_id = current_task['nodeId']
        current_task_id = current_task['taskId']
        current_init_id = current_task['initId']
        current_nodeConfig = current_task['nodeConfig']['configId']
        # Iterate over the task list to find dependent tasks
        for task in task_list:
            node_id = task['nodeId']
            task_id = task['taskId']
            init_Id = task['initId']
            tinst_id= task['taskInstId']
            nodeConfig = task['nodeConfig']['configId']
            # Check if the node id is less than the current node id
            # or if it's the same node but with a lesser task id
            if current_init_id==init_Id and (node_id < current_node_id or (node_id == current_node_id and current_nodeConfig==nodeConfig and task_id < current_task_id)):
                # Add the (nodeId, taskId) tuple to the dependent_tasks list
                current_task['dependent_tasks'].append(tinst_id)

    return sorted(task_list, key=lambda x: (len(x['dependent_tasks']),x['nodeId'], x['taskId']))


def get_node_list_dict(init, args):
   ## from orchestrator_utils.data_init_generic import  node_processes
    project_config = get_proj_configs(app = args.app_config, env = args.env)
    nodes_list = []
    if init:
        initializationId = get_json_attributes(init, 'initializationId', project_config)
        nodes_list = get_json_attributes(init, 'nodes', project_config)
        for node in nodes_list:
            node['initializationId'] = initializationId 

    return nodes_list

def get_tasks_list(init_file_list,args,project_config,run_id,failedConfigs=[]):
    node_processes = {
                'DataPreProcessNode'       : ['dpp','data_transformation'],
                'DataIngestionNode'        : ['di','data_ingestion'],
                'DataQualityNode'          : ['dq','data_quality'],
                'DataTransformationNode'   : ['dt','data_transformation'],
                #'DataMaskingNodeNode'     : ['dm','data_masking', data_masking.data_masking],
                #'DataDeAnonymizationNode' : ['da','data_de_anonymization', data_de_anonymization.data_de_anonymization],
                'DataPersistenceNode'      : ['dp','data_persistence'],
                'DataPipelineNode'         : ['dpl','data_pipeline']
}
    tasks_list=[]
    for init in init_file_list: 
        initId = int(time.time())
                            
        nodes_list = get_node_list_dict(init, args)

        for node in nodes_list:
            initialization_id=node['initializationId']
            config_id  = node['configuration']['configId']
            if len(failedConfigs) >0 and (initialization_id,config_id) not in failedConfigs:
                    continue

            print(node, initId)
            node['initId'] =initId  
            process_nm = node_processes[node['nodeName']][1]
            
            _, proc_config = get_config_filename(project_config.get("config_path").format(process_nm), config_id , project_config)
            node['nodeConfig'] = node.pop('configuration')
            node['nodeDescr'] = node.pop('description')
            if(proc_config==None):
                None#raise Exception("Node details not defined")
            elif 'tasks' in proc_config:
                for task in proc_config['tasks']:
                    tnode = {**node, **proc_config}
                    del tnode['tasks']
                    tnode= {**tnode, **task}
                    tnode['execStamp'] = None
                    tnode['taskStarted'] = None
                    tnode['taskEnded'] = None
                    tnode['taskInstId'] = utils.string_to_hex(f"{run_id}-{initId}-{tnode['nodeConfig']['configId']}-{tnode['xform_id']}")
                    tnode['status']   = None
                    tnode['restarts'] = 0
                    tnode['error']    = None
                    tasks_list.append(tnode) 
                    
            else:
                
                tnode = {**node, **proc_config}
                tnode['taskId']   = '1'
                tnode['xform_id'] = tnode['configId']
                tnode['taskName'] = tnode['name']
                tnode['taskType'] = tnode['nodeName']
                tnode['execStamp'] = None
                tnode['taskStarted'] = None
                tnode['taskEnded'] = None
                tnode['taskInstId'] = utils.string_to_hex(f"{run_id}-{initId}-{tnode['nodeConfig']['configId']}-{tnode['xform_id']}")
                tnode['status']   = None
                tnode['restarts'] = 0
                tnode['error']    = None
                tasks_list.append(tnode)
    tasks_list=add_dependent_tasks(tasks_list)
    return tasks_list   


def add_config_item(config, dot_separated_key_path, value):
    """
    Adds a value at a specified nested key path (dot-separated) in a dictionary.

    :param egress_config: The original dictionary to modify.
    :param dot_separated_key_path: String of dot-separated keys representing the path to set the value.
    :param value: The value to set at the specified key path.
    :return: The updated dictionary.
    """
    keys = dot_separated_key_path.split(".")

    def set_nested_value(d, keys, val):
        """ Helper function to set value at nested key path. """
        for key in keys[:-1]:
            if key not in d or not isinstance(d[key], dict):
                d[key] = {}
            d = d[key]
        d[keys[-1]] = val

    set_nested_value(config, keys, value)
    return config


def save_dicts_to_json(list_of_dicts, filename, file_location, append_mode=False):
    # Construct the full path with filename
    os.makedirs(file_location, exist_ok=True)
    full_path = os.path.join(file_location, filename).replace("//", "/")
    existing_data =[]
    if append_mode:
        if os.path.exists(full_path):
            with open(full_path, 'r') as json_file:
                existing_data = json.load(json_file)
        
        # Combine existing data with new data
        updated_data = existing_data + list_of_dicts
        
        # Write the combined data back to the file
        with open(full_path, 'w') as json_file:
            json.dump(updated_data, json_file, indent=4)
    else:
        # Overwrite mode
        with open(full_path, 'w') as json_file:
            json.dump(list_of_dicts, json_file, indent=4)
    
    action = 'appended to' if append_mode else 'saved to'
    print(f"List of dictionaries has been {action} '{full_path}'")


def log_tasks(tasks_list,run_id,log_path):
    logfileLocation=f"/dbfs{log_path}/{run_id}"
    filename = f"history_log.json"  # Name of the JSON file
    save_dicts_to_json(tasks_list, filename, logfileLocation,append_mode=True)
    filename = f"last_run_log.json"  # Name of the JSON file
    save_dicts_to_json(tasks_list, filename, logfileLocation,append_mode=False)
    
    failedConfigs=[]
    for task in tasks_list:
        if task['status']!='success':
            print(task['status'])
            initializationId,configId=task['initializationId'],task['configId']
            failedConfigs.append((initializationId,configId))
    failedConfigs=list(set(failedConfigs))
    filename = f"last_failed_config.json" 
    save_dicts_to_json(failedConfigs, filename, logfileLocation,append_mode=False)
    
def get_failed_configs(run_id,log_path):
    logfileLocation=f"/dbfs{log_path}/{run_id}"
    filename = f"last_failed_config.json" 
    full_path = os.path.join(logfileLocation, filename).replace("//", "/")
    
    # Ensure the file exists 
    if not os.path.exists(full_path):
        print(f"File not found: {full_path}")
        return []
    
    # Open and read the JSON file
    with open(full_path, 'r') as json_file:
        # Read the data from the file
        data = json.load(json_file)
    
    # Check if the data is a list
    if isinstance(data, list):
        # Convert each inner list to a tuple
        data_as_tuples = [tuple(item) for item in data]
        return data_as_tuples
    else:
        print("The file content is not a list.")
        return []


#demo code added by yasir jan24  --end
