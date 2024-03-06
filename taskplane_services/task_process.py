from pyspark.sql import SparkSession, utils as spark_utils, DataFrame
from pyspark.sql.types import *
spark= SparkSession.builder.getOrCreate()
from common_utils import utils ,batch_process_utils

task_pro_dict={
"DataIngestionNode"         : ["task_dataplane"         ,"t_batch_data_ingestion"        ,""],    ## accepts config                         ##param format(config, df ,process_name)  #return none
"DataQualityNode"           : ["task_dataplane"         ,"t_batch_data_quality"          ,""],
"DataTransformationNode"    : ["task_dataplane"         ,"t_batch_data_transformation"   ,""],
"DataPersistenceNode"       : ["task_dataplane"         ,"t_batch_data_persistence"      ,""],
"source_read"               : ["task_df_generate"       ,"t_source_df_read"              ,""],    ## accepts config      and  generate df   ##param format(config,     process_name)  #return df
"target_load"               : ["task_df_consume"        ,"t_file_storage_write"          ,""],    ## accepts config , df                    ##param format(config, df ,process_name)  #return none
"data_transform"            : ["task_df_transform"      ,"t_task_batch_data_transformation"            ,""],    ## accepts config , df                    ##param format(config, df ,process_name)  #return df
"join"                      : ["task_df_multi_in"       ,"t_task_batch_join_df" ,""],    ## accepts config ,[df1,df2...]           ##param format(config,[df],process_name)  #return 
"generaltransform": ["task_df_transform","<cpython func name>"            ,""],

}
#############################################################################################################################
 

import threading
import uuid
import time
from datetime import datetime
import traceback

class TaskProcessor:
    def __init__(self, tasks_list,max_threads):
        self.completed_tasks =  [item['taskInstId'] for item in tasks_list if item['status'] == 'success']
        self.initiated_tasks = []
        self.failed_tasks     =[]

        self.dfs = {}
        self.taskStamps = {}
        self.pending_tasks = [item['taskInstId'] for item in tasks_list if item['status'] == None]
        #this initiation is conditional. to be edited for restarts
        
        self.tasks_list = tasks_list
        self.lock = threading.Lock()
        self.max_threads = max_threads

    def exec_task(self,task):
        
        task_type,task_name,config=task['taskType'],task['taskName'],task['configuration']
        proc_type,proc_name, proc_lib=task_pro_dict[task_type][:3]

        if proc_type=="task_df_generate":
            proc_call=f"{proc_name}(config,'{task_name}')"
            print(proc_call)
            self.dfs[config["egressDf"]]=eval(proc_call)

        elif proc_type=="task_df_consume":
            proc_call = f"{proc_name}(config,self.dfs['{config['ingressDf']}'],'{task_name}')"
            print(proc_call)
            eval(proc_call)

        elif proc_type=="task_df_transform":
            proc_call= f"{proc_name}(config,self.dfs['{config['ingressDf']}'],'{task_name}')"
            print(proc_call)
            self.dfs[config["egressDf"]]=eval(proc_call)

        elif proc_type=="task_df_multi_in":
            proc_call= f"{proc_name}(config,self.dfs['{config['ingressDf1']}'],self.dfs['{config['ingressDf2']}'],'{task_name}')"
            print(proc_call)
            self.dfs[config["egressDf"]]=eval(proc_call)


        elif proc_type=="task_dataplane":
            proc_call=f"{proc_name}(config,'{task_name}')"
            print(proc_call)
            eval(proc_call)
        else:
            print(f"{proc_type}  not defined")
        
        #if proc_type=="task_df_multi_in":
        #    proc_call=f"{proc_name}(config,'{task_name}')"
        #    eval(proc_call)   ##### create array of  df  as parameter 
        

    def process_task(self, task):
        try:

            if len([item for item in task['dependent_tasks'] if item  in self.failed_tasks]) > 0:
                task['status'] = "skipped"
                if task['taskInstId'] in self.pending_tasks:
                    self.pending_tasks.remove(task['taskInstId'])
                self.failed_tasks.append(task['taskInstId'])
                return None

        
            if len([item for item in task['dependent_tasks'] if item not in self.completed_tasks]) == 0:
            
                execStamp = str(uuid.uuid4())
                if task['taskInstId'] not in self.taskStamps:
                    self.taskStamps[task['taskInstId']] = execStamp
                    task['taskStarted'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if self.taskStamps[task['taskInstId']]  ==execStamp:
                    print("running:", task['taskInstId'])
                    self.pending_tasks.remove(task['taskInstId'])
                    self.initiated_tasks.append(task['taskInstId'])
                    self.exec_task(task)
                    task['taskEnded'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    print("Complete:", task['taskInstId'])
                    self.completed_tasks.append(task['taskInstId'])
                    self.initiated_tasks.remove(task['taskInstId'])

                    task['status'] = "success"
                    return None
                else:
                    return None 


        except Exception as e:
            print(f"Error encountered: {e}, restarting task")
            try:
                # Add any additional logic needed for restarting the task
                task['status'] = "restarted"
                task['restarts'] = 1

                # Retry the execution
                self.exec_task(task)

            except Exception as e:
                print(f"Failed on restart: {e}")
                task['status'] = "failed"
                task['error']=str(traceback.format_exc())
                # Perform the required actions on failure
                self.failed_tasks.append(task['taskInstId'])
                if task['taskInstId'] in self.initiated_tasks:
                    self.initiated_tasks.remove(task['taskInstId'])
            #    if task['taskInstId'] in self.pending_tasks:
            #        self.pending_tasks.remove(task['taskInstId'])
                return  # Exit the function as it has failed



    def process_tasks(self):
        threads = []
        while self.pending_tasks:
            task= next((item for item in self.tasks_list if item['taskInstId'] in self.pending_tasks),None)
            if task is None:
                break 
            threads = [t for t in threads if t.is_alive()]
            time.sleep(.5)
            if task['taskInstId'] in self.pending_tasks:
                if len(threads) < self.max_threads:
                    task_thread = threading.Thread(target=self.process_task, args=(task,))
                    task_thread.start()
                    threads.append(task_thread)
                else:
                    while len(threads) >= self.max_threads:
                        time.sleep(.5) 
                        threads = [t for t in threads if t.is_alive()]
                    
        for thread in threads:
            thread.join()
        return self.tasks_list










#############################################taskplane functions can be moved to anothe py file###############################

from dataplane_services import batch_data_ingestion as bi ,batch_data_quality as bq,batch_data_transformation as bt,batch_data_persistence as bp
from common_utils import batch_process_utils as bpu
from configs import  app_config as cnfg
def t_batch_data_ingestion(config,task_name=''):
    bi.batch_data_ingestion(config)
def t_batch_data_quality(config,task_name=''):
    bq.batch_data_quality(config)
def t_batch_data_transformation(config,task_name=''):
    bt.batch_data_transformation(config)
def t_batch_data_persistence(config,task_name=''):
    bp.batch_data_persistence(config)
def t_source_df_read(config,task_name=''):
    df=bpu.source_df_read(config, task_name) 
    return df
def t_file_storage_write(config,df,task_name=''):
    config=cnfg.add_config_item(config,"data.inputFile.batchFlag","false" if df.isStreaming else "true")
    df=bpu.file_storage_write(task_name, df, config)
    return df
def t_task_batch_data_transformation(config,df,task_name=''):
    df=bt.task_batch_data_transformation( df, config)
    return df
def t_task_batch_join_df(config,df1,df2,task_name=''):
    df1.createOrReplaceTempView(f"tbl_{config['ingressDf1']}")
    df2.createOrReplaceTempView(f"tbl_{config['ingressDf2']}")
    df=spark.sql(config['query_override'])

    return df



