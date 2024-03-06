from taskplane_services.task_process import *
from common_utils import  utils as utils
from configs.app_config import get_proj_configs,set_client_functions,get_tasks_list,get_init_list,get_node_list,log_tasks,get_failed_configs
import argparse
import traceback
import time
import sys
from uuid import uuid4

def get_arguments():
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')
    parser.add_argument('-a', '--app_config', dest='app_config', required = False, default="default",
                        help='application config')
    parser.add_argument('-e', '--environment', dest='env', required = False, default="demo",
                        help='application environment')
    parser.add_argument('-c', '--config-path', dest='conf_path', required = True,
                        help='parent path for the configuration files')
    parser.add_argument('-bm', '--bookmark_flag', dest='bookmark_flag', required = False, action='store_true', 
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
    conf_dpl = subparser.add_parser('dpl')

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

    group_dpl = conf_dpl.add_mutually_exclusive_group()
    group_dpl.add_argument('-id', '--conf-id', dest='conf_id',
                        help=id_descriptor)
    group_dpl.add_argument('-if', '--init-file', dest='init_file', nargs='+',
                        help=if_descriptor)
    args = parser.parse_args()

    return args





def tasks_list_gen(args):

    
    run_id = str(uuid4())
        #demo code takes random uuid for runid. to ensure uniqueness this can be a function of time and app 
    v_project_config = get_proj_configs(args.app_config, args.env)
    v_project_config["run_id"]=run_id
    client, processing_engine, get_content, get_list, clear_files, move_files, write_file,copy_files = set_client_functions(v_project_config)
    config_root = args.conf_path if args.conf_path[-1:] == '/' else args.conf_path + '/'
    config_path  = config_root + '{}/'
    v_project_config["config_path"]=config_path
    reconcile_path = v_project_config.get("reconcile_path","")
    log_path = v_project_config.get("log_path","")
    #fsm_flag = args.fsm_flag
    #ers_flag = args.ers_flag
    clear_metadata = args.clear_flag
    old_target_count = 0
    init_process_nm = "Data Initialization"
    init_event_id  = ""
    init_file_list = get_init_list(args)
    print(init_file_list)
    tasks_list=[]
    configlist=[]
    if args.bookmark_flag==True :
        if args.run_id==None:
            print("No previous runid found.Switching to full run")
        else:
            run_id=args.run_id 
            configlist=get_failed_configs(run_id,log_path) 
            if len(configlist)==0:
                print("No fails found with previus runid.Switching to full run")

        #read previos task list
        #identify failed init id configid
        #v_project_config.failedTasks=""
    for init in init_file_list: 
        initId = int(time.time())
        #nodes_list = get_node_list(init, args)    
        tasks_list+=get_tasks_list(init_file_list,args,v_project_config,run_id,configlist)
    return tasks_list,v_project_config


def initilization_process():
    args = get_arguments()
    tasks_list,v_project_config=tasks_list_gen(args)
    task_processor = TaskProcessor(tasks_list,3)
    tasks_list=task_processor.process_tasks()
    log_tasks(tasks_list, v_project_config["run_id"],v_project_config.get("log_path",""))
    dfcounts={}
    for dfname,df in task_processor.dfs.items():
        dfcounts[dfname]=int(df.count())
    





