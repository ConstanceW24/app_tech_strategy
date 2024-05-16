from pyspark.sql import SparkSession
from datetime import datetime
import time
spark= SparkSession.builder.getOrCreate()
 
 
def get_task_status(task_id, schema = 'default'):
    df = spark.sql(f"SELECT task_status FROM {schema}.task_history WHERE task_id = '{task_id}' ")
    return df.take(1)[0].task_status
 
def get_task_complete_status(batch_id, schema = 'default'):
    df = spark.sql(f"SELECT count(1) inprogress_cnt FROM {schema}.task_history WHERE task_status != 'COMPLETED' and  batch_id = {batch_id}")
    return df.take(1)[0].inprogress_cnt
 
def update_task_status(task_id, status, schema = 'default', message = 'error', retry = 3):
    import datetime
 
    if status.upper() in ( 'IN-PROGRESS', 'COMPLETED', 'FAILED', 'RESTARTED') :
        try:
            current_batch_timestamp = datetime.datetime.now()
            prev_task_status = get_task_status(task_id, schema)
    
            if prev_task_status == 'FAILED' :
                status = 'RESTARTED'
                end_tmsp = 'NULL'
                start_tmsp =  f"'{current_batch_timestamp}', "
            elif status.upper() == 'IN-PROGRESS':
                start_tmsp =  f"'{current_batch_timestamp}', "
                end_tmsp = 'NULL'
            else :
                end_tmsp = f"'{current_batch_timestamp}'"
                start_tmsp = 'task_start_timestamp_utc, '

            
            if status.upper() == 'FAILED':
                start_tmsp =   start_tmsp + " task_message = '" + message[:200].replace('\'','') + "', "
    
            qry = f"UPDATE {schema}.task_history SET task_status = '{status.upper()}', task_start_timestamp_utc = {start_tmsp} task_end_timestamp_utc = {end_tmsp}  WHERE task_id = '{task_id}'"
            spark.sql(qry)
    
            return task_id
        except Exception as e:
            if ("ConcurrentAppendException" in str(e) or "concurrent" in str(e).lower()) and retry > 0 :
                print("Errored - reattempted {0}".format(str(e)))
                update_task_status(task_id, status, schema = 'default', message = 'error', retry = retry - 1)
            else:
                raise Exception(e)
        
    else:
        raise Exception(f"Unknown Status - {status}")
 
def insert_task_history(appl_short_name , source, schema = 'default'):

    qry = f"""INSERT INTO {schema}.task_history (task_id, batch_id, table_load_name, etl_layer, load_sequence_nbr, data_layer, task_start_timestamp_utc, extract_start_timestamp_utc, 
                    extract_end_timestamp_utc, task_status)
                  WITH task_max_start_batch as
                    (SELECT 
                    table_load_name,
                    load_sequence_nbr,
                    data_layer,
                    max(extract_end_timestamp_utc) +  interval '1 millisecond'  extract_start_timestamp
                    FROM
                    {schema}.task_history
                    GROUP BY 1,2,3)
                  SELECT uuid(), batch.batch_id, task_ref.table_load_name, task_ref.etl_layer, task_ref.load_sequence_nbr, task_ref.data_layer, current_timestamp(), 
                  coalesce(task_max.extract_start_timestamp, cast('1900-01-01' as timestamp)), 
                  date_trunc('minute', current_timestamp - interval '10 minute')- interval '1 millisecond' extract_end_timestamp,'SCHEDULED'
                  FROM {schema}.batch_history as batch 
                  JOIN {schema}.application_reference app_ref
                  ON (upper(batch.application_short_name) = upper(app_ref.application_short_name)  AND upper(batch.source_category) = upper(app_ref.source_category))
                  JOIN {schema}.application_task_reference task_ref
                  ON (app_ref.app_id = task_ref.app_id)
                  LEFT JOIN task_max_start_batch task_max
                  ON (task_ref.table_load_name = task_max.table_load_name and task_max.load_sequence_nbr = task_ref.load_sequence_nbr and task_max.data_layer = task_ref.data_layer  )
                  WHERE upper(batch.batch_status) in ( 'IN-PROGRESS' , 'RESTARTED' ) AND upper(batch.application_short_name) = upper('{appl_short_name}') 
                  AND upper(batch.source_category) = upper('{source}')
                  AND batch.batch_id not in (SELECT batch_id FROM {schema}.task_history )
                  AND coalesce(task_max.extract_start_timestamp, cast('1900-01-01' as timestamp)) < date_trunc('minute', current_timestamp - interval '10 minute')- interval '1 millisecond'"""
    
    #print(qry)
    spark.sql(qry)
 
 
def generate_batch_id(appl_short_name , source):
    from datetime import datetime
    epoch_timestamp = int(datetime.now().timestamp()*1000000)
    segment = appl_short_name + source
    ascii_values = [ord(char) for char in segment]  
    total = sum(ascii_values) % 1000
    batch_id = int(str(epoch_timestamp)  +  str(total).zfill(5))
    print(f"Assigning - {batch_id}")
    return batch_id
 
def verify_application(appl_short_name , source, schema = 'default'):
    df = spark.sql(f"SELECT COUNT(1) as cnt FROM {schema}.application_reference WHERE upper(application_short_name) = upper('{appl_short_name}') AND upper(source_category) =upper('{source}')")
    if df.take(1)[0].cnt == 0 :
        raise Exception("Application Reference Not Found")
 
def get_batch_status(batch_id, schema = 'default'):
    df = spark.sql(f"SELECT batch_status FROM {schema}.batch_history WHERE batch_id = {batch_id} ")
    return df.take(1)[0].batch_status
 
def get_max_batch_id(appl_short_name , source, status = None, schema = 'default'):
    qry = f"SELECT COALESCE(MAX(batch_id), -1) as batch_id FROM {schema}.batch_history WHERE upper(application_short_name) = upper('{appl_short_name}') AND upper(source_category) = upper('{source}')"
    if status :
        qry = qry + f" AND upper(batch_status) = upper('{status}') "
    df = spark.sql(qry)
    try:
        return df.take(1)[0].batch_id  
    except Exception as e:
        print(str(e))
        return -1
 
def update_batch_status(appl_short_name , source, status, schema = 'default', message = None):
    import datetime
 
    if status.upper() in ( 'IN-PROGRESS', 'COMPLETED', 'FAILED', 'RESTARTED') :
        current_batch_id = get_max_batch_id(appl_short_name , source, schema = schema)
        prev_batch_status = get_batch_status(current_batch_id, schema)
        current_batch_timestamp = datetime.datetime.now()
 
        if prev_batch_status == 'FAILED' :
            status = 'RESTARTED'
            end_tmsp = 'NULL'
        else :
            end_tmsp = f"'{current_batch_timestamp}'"

        if message :
             end_tmsp =  end_tmsp + f", batch_message = '{message}' "

        if status.upper() == 'COMPLETED':
            print("Ensuring all task are in completed state")
            inprogress_cnt = get_task_complete_status(current_batch_id, schema)
            if inprogress_cnt > 0:
                msg = f"{inprogress_cnt} tasks are not in completed status. So unable to mark a batch as completed"
                update_batch_status(appl_short_name , source, 'FAILED', schema, msg)
                raise Exception(msg)
 
        qry = f"UPDATE {schema}.batch_history SET batch_status = '{status.upper()}', batch_end_timestamp_utc = {end_tmsp}  WHERE batch_id = {current_batch_id}"
        spark.sql(qry)
 
        return current_batch_id
    else:
        raise Exception(f"Unknown Status - {status}")
 
def insert_batch_history(appl_short_name , source, schema = 'default'):
    import datetime
    verify_application(appl_short_name , source, schema)
    old_batch_id = get_max_batch_id(appl_short_name , source, schema = schema)
    #print(old_batch_id)
   
    if old_batch_id != -1 :
        prev_batch_status = get_batch_status(old_batch_id, schema)
        if prev_batch_status != 'COMPLETED' :
            if prev_batch_status == 'FAILED':
                prev_batch_id = update_batch_status(appl_short_name , source, 'IN-PROGRESS', schema)
                return prev_batch_id
            else:
                raise Exception(f"There is an already an open batch for {appl_short_name} - {source}")
   
    #batch_id = generate_batch_id(appl_short_name , source)
    start_timestamp = datetime.datetime.now()
    spark.sql(f"""INSERT INTO {schema}.batch_history (application_short_name, source_category, batch_start_timestamp_utc, batch_status)
                  VALUES ('{appl_short_name}', '{source}', '{start_timestamp}', 'IN-PROGRESS')""")
   
    insert_task_history(appl_short_name , source, schema)

    batch_id = get_max_batch_id(appl_short_name , source, schema = schema)
   
    return batch_id
 

def get_task_details(appl_short_name , source, schema = 'default', table_name = None, load_sequence_nbr = None, layer = None):

    qry = f"""
    with trans_rules as (select app_id,  table_load_name, etl_layer, load_sequence_nbr, collect_list(to_json(struct(rule_sequence_nbr as rule_id, rule_parser, rule_override, 
        coalesce(transformation_name,'') as Transformation_name , coalesce(transformation_input,'[]') as Transformation_input, 
        coalesce(transformation_output,'[]') as Transformation_output))) as trans_rule  
        from {schema}.application_transformation_reference trans
        where  trans.active_status = 'Y'
        group by app_id,  table_load_name, etl_layer,  load_sequence_nbr ),
    dq_rules as (select app_id,  table_load_name, etl_layer, load_sequence_nbr, collect_list(to_json(struct(coalesce(rule_sequence_nbr,'') as rule_id, 
        coalesce(data_field,'') as field_name, coalesce(rule_parser,'') rule_parser, 
        coalesce(attribute_value,'') default_value, coalesce(rule_override,'') rule_override, 
        coalesce(validation_name,'') validation_name, coalesce(validation_input,'') validation_input, 
        coalesce(validation_output,'') as validation_output_field, 
        coalesce(exception_handling,'Ignore') exception_handling)))   as dq_rules
        from {schema}.application_quality_reference quality
        where quality.active_status = 'Y'
        group by app_id,  table_load_name, etl_layer,  load_sequence_nbr )  
    select  hist.batch_id, hist.task_id, hist.Table_load_name, hist.load_sequence_nbr, ref.etl_layer, ref.data_layer, 
    ref.source_type, ref.source_format, ref.source_table, ref.source_option,
    ref.target_type, ref.target_format, ref.target_table, ref.target_option,
    coalesce(ref.reject_table, '') reject_table, 
    coalesce(ref.reject_option,'{{}}') reject_option, 
    coalesce(ref.validation_log_table,'') validation_log_table, 
    coalesce(ref.validation_log_option,'{{}}') validation_log_option,
    case when transformation_flag = 'Y' then coalesce(trans.trans_rule,array()) else array() end trans_rules, 
    case when quality_flag = 'Y' then coalesce(dq_rules.dq_rules,array()) else array() end  dq_rules,
    coalesce(app.validation_summary_option,'{{}}') validation_summary_option, 
    coalesce(app.reconcilation_summary_option,'{{}}') reconcilation_summary_option,
    hist.extract_start_timestamp_utc extract_start_timestamp, hist.extract_end_timestamp_utc extract_end_timestamp
    from 
    {schema}.task_history hist 
    join {schema}.application_task_reference ref on ( upper(hist.table_load_name) = upper(ref.table_load_name) and hist.load_sequence_nbr = ref.load_sequence_nbr  and hist.etl_layer = ref.etl_layer )
    join {schema}.application_reference app on (ref.app_id = app.app_id)
    join {schema}.batch_history batch on ( hist.batch_id = batch.batch_id)
    left join trans_rules trans on (ref.app_id = trans.app_id and upper(ref.table_load_name) = upper(trans.table_load_name)  and ref.load_sequence_nbr = trans.load_sequence_nbr and ref.etl_layer = trans.etl_layer)
    left join dq_rules on (ref.app_id = dq_rules.app_id and  upper(ref.table_load_name) = upper(dq_rules.table_load_name)  and ref.load_sequence_nbr = dq_rules.load_sequence_nbr and ref.etl_layer = dq_rules.etl_layer)
    where upper(batch_status) != 'COMPLETED' and upper(task_status) != 'COMPLETED' 
    and upper(app.application_short_name) = upper('{appl_short_name}') 
    and upper(app.source_category) =  upper('{source}') and ref.active_status = 'Y'
    """
    
    if table_name :
        qry = qry + f" and upper(ref.table_load_name) = upper('{table_name}')"

    if load_sequence_nbr :
        qry = qry + f" and ref.load_sequence_nbr = '{load_sequence_nbr}'"

    if layer :
        qry = qry + f" and upper(ref.etl_layer) = upper('{layer}')"

    qry = qry + " ORDER BY 1,5,4"
    df = spark.sql(qry)

    return df




# Config Generators


def rule_sort(rule_list):
    import json
    t_rules = []
    for rule_set in rule_list:
      new_rules = json.loads(rule_set)
      print(new_rules)
      if 'Transformation_input' in new_rules.keys():
        print(new_rules['Transformation_input'])
        new_rules['Transformation_input'] = json.loads(new_rules['Transformation_input'])
      if 'Transformation_output' in new_rules.keys():
        print(new_rules['Transformation_output'])
        new_rules['Transformation_output'] = json.loads(new_rules['Transformation_output'])
      t_rules = t_rules + [new_rules]
    trans_rules = sorted(t_rules, key = lambda x: x['rule_id'])
    return trans_rules


def get_task_config(record):
    import json
    source_option = json.loads(record['source_option'])
    source_option.update({'table':record['source_table']})
    source_option.update({'format':record['source_format']})

    target_option = json.loads(record['target_option'])
    target_option.update({'table':record['target_table']})
    target_option.update({'format':record['target_format']})

    target_reject_option = json.loads(record['reject_option'])
    if target_reject_option != {} :
        if record['reject_table']!= '':
            target_reject_option.update({'table':record['reject_table']})
    else:
        target_reject_option = None
    
    target_log_option = json.loads(record['validation_log_option'])
    if target_log_option != {} :
        if record['validation_log_table']!= '':
            target_log_option.update({'table':record['validation_log_table']})
    else:
        target_log_option = None
    
    target_summary_option = json.loads(record['validation_summary_option'])
    if target_summary_option == {} :
        target_summary_option = None

    target_reconcilation_option = json.loads(record['reconcilation_summary_option'])
    if target_reconcilation_option == {} :
        target_reconcilation_option = None

    trans_rules = rule_sort( json.loads(json.dumps(record['trans_rules']).replace("<<batch_id>>",str(record['batch_id'])).replace("<<extract_start_timestamp>>",str(record['extract_start_timestamp'])).replace("<<extract_end_timestamp>>",str(record['extract_end_timestamp']))))
    
    dq_rules = rule_sort( json.loads(json.dumps(record['dq_rules']).replace("<<batch_id>>",str(record['batch_id'])).replace("<<extract_start_timestamp>>",str(record['extract_start_timestamp'])).replace("<<extract_end_timestamp>>",str(record['extract_end_timestamp']))))
    


    config = {
            "configuration": {
                "run_id" : record['task_id'],
                "table_name" : record['Table_load_name'],
                "data_layer" : record['data_layer'],
                "batch_id" : record['batch_id'],
                "start_time" : datetime.now(),
                "extract_start_timestamp": record['extract_start_timestamp'],
                "extract_end_timestamp": record['extract_end_timestamp'],
                "data": {
                "inputFile": {
                    "schema": "NA",
                    "options": source_option,
                    "fileName": record['Table_load_name'],
                    "inputFileFormat":  record['source_format'],
                    "batchFlag": "true"
                },
                "eventTypeId": record['Table_load_name']
                },
                "status" : "active",
                "rules": trans_rules,
                "dqrules":{"postdtchecks": "true", "rules" : dq_rules},
                "source": {
                "driver": {
                    "path": source_option.get('path', ''),
                    "table": source_option.get('table', ''),
                    "format": record['source_format'],
                    "SourceType": record['source_type'],
                    "checkpointLocation": ""
                },
                "dependents": []
                },
                "target": {
                "options": target_option,
                "rejectOptions" : target_reject_option,
                "validationLogOptions" : target_log_option,
                "rejectSummaryOptions" : target_summary_option,
                "reconcilationOptions" : target_reconcilation_option,
                "targetType": record['target_type'],
                "targetTrigger": "once"
                }
            }
            }
    
    print(config)
    return config
    

def insert_batch():
    import argparse
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')

    parser.add_argument('-a', '--appl_short_name', dest='appl_short_name', required = True, 
                        help='pass Application short name')
    parser.add_argument('-src', '--source', dest='source', required = True, 
                        help='pass source for run')
    parser.add_argument( '-db','--schema', dest='schema', required = False, default='edw_shared', 
                        help='pass schema for dp run')

    args = parser.parse_args()
    print(args)

    return insert_batch_history(args.appl_short_name.upper() , args.source.upper(), args.schema.upper())

def update_batch():
    import argparse
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')

    parser.add_argument('-a', '--appl_short_name', dest='appl_short_name', required = True, 
                        help='pass Application short name')
    parser.add_argument('-src', '--source', dest='source', required = True, 
                        help='pass source for run')
    parser.add_argument('-sts', '--status', dest='status', required = True, 
                        help='pass status for batch to update')
    parser.add_argument( '-db','--schema', dest='schema', required = False, default='edw_shared', 
                        help='pass schema for dp run')

    args = parser.parse_args()
    print(args)
    
    return  update_batch_status(args.appl_short_name.upper() , args.source.upper(), args.status.upper(), args.schema.upper())


