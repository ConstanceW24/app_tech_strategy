from pyspark.sql import SparkSession
from datetime import datetime
import time
spark= SparkSession.builder.getOrCreate()
 


def cleanup_tables( batch_id , schema='default', layer = None, table_name = None):

    qry = f"""
    select
    "DELETE FROM " || appref.target_table || " WHERE batch_id = " || taskhist.batch_id || ';' statement
    from
    {schema}.task_history taskhist
    inner join {schema}.application_task_reference appref 
    on (
    taskhist.table_load_name = appref.table_load_name
    and taskhist.etl_layer = appref.etl_layer)
    where taskhist.batch_id = {batch_id}
    """
    
    if table_name :
        qry = qry + f" and upper(appref.table_load_name) = upper('{table_name}')"

    if layer :
        qry = qry + f" and upper(appref.etl_layer) = upper('{layer}')"

    qry = qry + " ORDER BY appref.etl_layer"
    df = spark.sql(qry)

    exec_statement(df)


def repair_tables( appl_short_name , source, schema = 'default', option = 'OPTIMIZE', layer = None, table_name = None):

    qry = f"""
    SELECT
    "{option} " || task_ref.target_table || ";" statement, *
    FROM
    application_task_reference task_ref
    inner join application_reference app_ref
    on (app_ref.app_id = task_ref.app_id)
    WHERE 
    upper(app_ref.application_short_name) = upper({appl_short_name})
    and 
    upper(app_ref.source_category) = upper({source})
    """
    
    if table_name :
        qry = qry + f" and upper(task_ref.table_load_name) = upper('{table_name}')"

    if layer :
        qry = qry + f" and upper(task_ref.etl_layer) = upper('{layer}')"

    qry = qry + " ORDER BY task_ref.target_table"
    df = spark.sql(qry)

    exec_statement(df)


def exec_statement(df):
    for rec in df.collect():
        qry = rec['statement']
        print(qry)
        spark.sql(qry)


def cleanup_audit( batch_id , schema='default'):

    qry = f""" DELETE FROM {schema}.task_history WHERE batch_id = {batch_id}; """
    spark.sql(qry)
    print("Cleared Task history")

    qry = f""" DELETE FROM {schema}.batch_history WHERE batch_id = {batch_id}; """
    spark.sql(qry)
    print("Cleared Batch history")

    vacuum_audit( schema)

def reset_audit( batch_id , schema='default', task_id = None, layer = None, table_name = None):

    condition = f"taskhist.batch_id = {batch_id}"
    if table_name :
        condition = condition + f" and upper(appref.table_load_name) = upper('{table_name}')"

    if task_id :
        condition = condition + f" and upper(taskhist.task_id) = '{task_id}'"

    if layer :
        condition = condition + f" and upper(appref.etl_layer) = upper('{layer}')"

    qry = f"""
    select
        part_1 || substr(part_2, 2, length(part_2) -2) || part_3 as statement
    from
    (
        select
        "UPDATE {schema}.task_history SET task_status = 'SCHEDULED' WHERE TASK_ID IN (" part_1,
        cast(
            collect_set("'" || taskhist.task_id || "'") as string
        ) part_2,
        ");" part_3
        from
        {schema}.task_history taskhist
        inner join {schema}.application_task_reference appref on (
            taskhist.table_load_name = appref.table_load_name
            and taskhist.etl_layer = appref.etl_layer
        )
        where  {condition} and  task_status != 'SCHEDULED'
    )    
    """

    df = spark.sql(qry)
    exec_statement(df)

    vacuum_audit( schema)



def vacuum_audit( schema='default'):

    qry = f"""VACUUM {schema}.task_history; """
    spark.sql(qry)
    qry = f"""OPTIMIZE {schema}.task_history; """
    spark.sql(qry)
    print("Vacuum Task history")

    qry = f"""VACUUM {schema}.batch_history; """
    spark.sql(qry)
    qry = f"""OPTIMIZE {schema}.batch_history; """
    spark.sql(qry)
    print("Vacuum Batch history")


### - Calling functions

def compact_tables():
    import argparse
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')
    parser.add_argument('-a', '--appl_short_name', dest='appl_short_name', required = True, help='pass Application short name')
    parser.add_argument('-src', '--source', dest='source', required = True, help='pass source for run')
    parser.add_argument( '-db','--schema', dest='schema', required = True, default='default', 
                        help='pass schema for dp run')
    parser.add_argument('-tbl','--table', dest='table', required = False, default = None , help='pass schema for dp run')
    parser.add_argument('-ly','--layer', dest='layer', required = False, default = None , help='pass sequence for dp run')
    parser.add_argument('-o','--option', dest='repair_option', required = True, choices=['vacuum', 'optimize'] , help='pass sequence for dp run')
    args = parser.parse_args()
    print(args)

    etl_layer = args.layer if args.layer != None and (args.layer).strip() != "" else None
    target_table = args.table if args.table != None and (args.table).strip() != "" else None

    repair_tables( appl_short_name = args.appl_short_name , source = args.source, schema = args.schema, option = args.repair_option, 
                  layer = etl_layer, table_name = target_table)



def clear_batch():
    import argparse
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')
    parser.add_argument( '-db','--schema', dest='schema', required = True, default='default', 
                        help='pass schema for dp run')
    parser.add_argument('-b', '--batch_id', dest='batch_id', required = True, 
                        help='pass Application short name')
    parser.add_argument('-tbl','--table', dest='table', required = False, default = None , help='pass schema for dp run')
    parser.add_argument('-ly','--layer', dest='layer', required = False, default = None , help='pass sequence for dp run')

    args = parser.parse_args()
    print(args)

    etl_layer = args.layer if args.layer != None and (args.layer).strip() != "" else None
    target_table = args.table if args.table != None and (args.table).strip() != "" else None

    cleanup_tables( args.batch_id , schema=args.schema, layer = etl_layer, table_name = target_table)
    print("Data tables Cleaned up")
    reset_audit( args.batch_id , schema=args.schema, layer = etl_layer, table_name = target_table)
    print("Audit tables Cleaned up")


def clear_audit():
    import argparse
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')
    parser.add_argument( '-db','--schema', dest='schema', required = True, default='default', 
                        help='pass schema for dp run')
    parser.add_argument('-b', '--batch_id', dest='batch_id', required = True, 
                        help='pass Application short name')

    args = parser.parse_args()
    print(args)

    cleanup_audit(args.batch_id , schema=args.schema)
    print("Audit tables Cleaned up")


    
def reset_task_history():
    import argparse
    parser = argparse.ArgumentParser(description='Process Data Pipeline.')
    parser.add_argument( '-db','--schema', dest='schema', required = True, default='default', 
                        help='pass schema for dp run')
    parser.add_argument('-t', '--task_id', dest='task_id', required = True, 
                        help='pass Application short name')
    parser.add_argument('-b', '--batch_id', dest='batch_id', required = True, 
                        help='pass Application short name')
    parser.add_argument('-tbl','--table', dest='table', required = False, default = None , help='pass schema for dp run')
    parser.add_argument('-ly','--layer', dest='layer', required = False, default = None , help='pass sequence for dp run')

    args = parser.parse_args()
    print(args)

    etl_layer = args.layer if args.layer != None and (args.layer).strip() != "" else None
    target_table = args.table if args.table != None and (args.table).strip() != "" else None

    reset_audit(batch_id = args.batch_id, task_id = args.task_id, schema=args.schema, layer = etl_layer, table_name = target_table)
    print("Audit tables Cleaned up")
    