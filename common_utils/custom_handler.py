import sys
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from common_utils import utils as utils

spark = SparkSession.builder.getOrCreate()

def skip_rows(df,rows):
    df = df.withColumn('idx', F.monotonically_increasing_id())
    df = df.filter(df['idx']>=rows).drop('idx')
    return df


def split_df(df,rows):
    df = df.withColumn('idx', F.monotonically_increasing_id())
    return df.filter(df['idx']<rows).drop('idx'), df.filter(df['idx']>=rows).drop('idx')


def get_rows(df,rows):
    df = df.withColumn('idx', F.monotonically_increasing_id() + 1)
    return list(df.filter(df['idx']==rows).drop('idx').take(1)[0].asDict().values())



def generate_columns_from_metadata(src_df, arg_input):
    """
    column_list (in form of dict)
    skiprows
    """
    print("Running custom handler: generate_columns_from_metadata")
    data_input = arg_input[0]

    headers = get_rows(src_df, int(data_input['header_pos']))

    print(headers)

    df = src_df.toDF(*headers)

    df = skip_rows(df, int(data_input['header_pos']))

    for col_detail in data_input['column_details']:
        print(col_detail)
        col_name = col_detail['column_name']
        col_value = get_rows(src_df, int(col_detail['value_position'][0]))[int(col_detail['value_position'][1])-1]

        if 'transformation' in col_detail:
            col_value = eval(col_detail['transformation'])

        print(col_name, col_value)
        df = df.withColumn(col_name, F.lit(col_value))

    return df


def key_generator(src_df, arg_output=[]):

    from datetime import datetime
    epoch_timestamp = int(datetime.now().timestamp()*1000000)

    column_nm = "PKID" if arg_output == [] else arg_output[0].strip().upper()

    from pyspark.sql.functions import concat, monotonically_increasing_id, lpad, lit    
    src_df = src_df.withColumn(column_nm, concat(lit(epoch_timestamp), lpad(monotonically_increasing_id(),12,'0')).cast("decimal(38,0)"))

    return src_df


def sort_and_rank(src_df, arg_input=[],scd_type = "scd2"):
    from datetime import datetime
    from pyspark.sql.window import Window
    from pyspark.sql.functions import current_timestamp, when, col, lead, lit , expr, row_number 


    key_field      = arg_input[0]['key_columns']
    order_field      = arg_input[0].get('order_columns',["CREATED_TIMESTAMP","UPDATED_TIMESTAMP"])
    compare_field      = arg_input[0].get('compare_columns',[])
    eff_date_field_map = arg_input[0].get('start_tmsp_column_map','CREATED_TIMESTAMP')
    eff_date_field_nm = arg_input[0].get('start_tmsp_column_nm','START_EFF_TIMESTAMP').upper()
    end_date_field_nm = arg_input[0].get('end_tmsp_column_nm','END_EFF_TIMESTAMP').upper()
    default_tmsp = arg_input[0].get('default_end_tmsp','9999-12-31')
    target_tbl = arg_input[0]['target_name']
    currnt_time = datetime.now()
    data_columns = compare_field if compare_field else [cl for cl in src_df.columns if cl not in key_field + order_field + [eff_date_field_map]]

    if 'CREATED_TIMESTAMP' not in src_df.columns :
        src_df = src_df.withColumn('CREATED_TIMESTAMP', lit(currnt_time))
    
    if 'UPDATED_TIMESTAMP' not in src_df.columns :
        src_df = src_df.withColumn('UPDATED_TIMESTAMP', col('CREATED_TIMESTAMP'))

    src_df = src_df.withColumn(eff_date_field_nm, col(eff_date_field_map))\
                    .withColumn('DATA_HASH', expr(f"md5(concat_ws('',{','.join(data_columns)}))"))
    
    if scd_type == 'scd1':
        latest_windowspec  = Window.partitionBy(*[col(k) for k in key_field]).orderBy(*[col(k).desc() for k in order_field])
        src_df = src_df.withColumn('LATEST_RNUM', row_number().over(latest_windowspec)).where("LATEST_RNUM = 1").drop("LATEST_RNUM")

    windowspec  = Window.partitionBy(*[col(k) for k in key_field]).orderBy(*[col(k).asc() for k in order_field])

    src_df = src_df.withColumn('NEXT_HASH', lead(col('DATA_HASH'),1,'').over(windowspec))\
                .where("NEXT_HASH != DATA_HASH").drop('NEXT_HASH')\
                .withColumn('least_rnum', row_number().over(windowspec))
    
    try:
        tgt_df = spark.sql(f"SELECT {','.join(key_field)}, DATA_HASH as TGT_DATA_HASH FROM {target_tbl} WHERE cast({end_date_field_nm} as date) = cast('{default_tmsp}' as date)")
        src_df = src_df.join(tgt_df, key_field, "left").where("data_hash <> coalesce(tgt_data_hash,'') and least_rnum = 1").select(src_df['*'])
        print(src_df.columns)
        
    except Exception as e:
        print(str(e))
        pass
    
    src_df = src_df.withColumn(end_date_field_nm, lead(col(eff_date_field_nm),1,default_tmsp).over(windowspec).cast('timestamp'))\
                .withColumn('ACTIVE_IND', when(col(end_date_field_nm)==lit(default_tmsp).cast('timestamp'),lit('Y')).otherwise(lit('N')))

    
    return src_df


def get_target_update(src_name, arg_input, scd_type):

    key_field      = arg_input[0]['key_columns']
    order_field      = arg_input[0].get('order_columns',["created_timestamp","updated_timestamp"])
    compare_field      = arg_input[0].get('compare_columns',[])
    exclude_field      = arg_input[0].get('exclude_columns',[])
    eff_date_field_map = arg_input[0].get('start_tmsp_column_map','created_timestamp')
    eff_date_field_nm = arg_input[0].get('start_tmsp_column_nm','start_eff_timestamp')
    end_date_field_nm = arg_input[0].get('end_tmsp_column_nm','end_eff_timestamp')

    default_tmsp = arg_input[0].get('default_end_tmsp','9999-12-31')
    target_tbl = arg_input[0]['target_name']

    src_df = spark.table(src_name)
    data_columns = compare_field if compare_field else [cl for cl in src_df.columns if cl not in key_field + order_field + exclude_field + [eff_date_field_map]]
    print(f"comparing - {data_columns}")

    if scd_type.lower() == 'scd2':
        return f"""MERGE INTO {target_tbl} TGT
                        USING(
                        SELECT md5(concat_ws("",{",".join(key_field)})) KEY, 
                        {eff_date_field_nm} START_TMSP,
                        md5(concat_ws("",{",".join(data_columns)})) DATA_HASH
                        FROM {src_name}
                        WHERE least_rnum = 1
                        ) SRC
                        ON (md5(concat_ws("",TGT.{",TGT.".join(key_field)})) = SRC.KEY AND TGT.ACTIVE_IND = 'Y')
                        WHEN MATCHED THEN
                        UPDATE 
                        SET {end_date_field_nm} = START_TMSP,
                        UPDATED_TIMESTAMP = CURRENT_TIMESTAMP(),
                        ACTIVE_IND = 'N';"""
        
    elif scd_type.lower() == 'scd1':
        
        return f"""MERGE INTO {target_tbl} TGT
                        USING(
                        SELECT md5(concat_ws("",{",".join(key_field)})) KEY,
                        md5(concat_ws("",{",".join(data_columns)})) DATA_HASH
                        FROM {target_tbl}_stage
                        WHERE least_rnum = 1
                        ) SRC
                        ON (md5(concat_ws("",TGT.{",TGT.".join(key_field)})) = SRC.KEY AND CAST(TGT.{end_date_field_nm} as DATE) = CAST('{default_tmsp}' as DATE))
                        WHEN MATCHED THEN
                        DELETE;"""



def expand( data_type, col_nm = '', column_map_list = []):
    import pyspark
    from pyspark.sql.functions import posexplode_outer, col
    for i in data_type:
        if isinstance(i.dataType,pyspark.sql.types.StructType):
            sec_nm = col_nm
            if col_nm != '':
                sec_nm = col_nm + '~'
            column_map_list += expand( i.dataType, sec_nm + i.name, [])
        elif isinstance(i.dataType,pyspark.sql.types.ArrayType) :
            sec_nm = col_nm
            if col_nm != '':
                sec_nm = col_nm + '~'
            column_map_list += [( posexplode_outer(col(f"{sec_nm.replace('~','.')}{i.name}")), ((f"{sec_nm}{i.name}~POS"),f"{sec_nm}{i.name}"))]
        else :
            if col_nm != '':
                column_map_list += [( col(f"{col_nm.replace('~','.')}.{i.name}"), (col_nm + '~'+ i.name))]
            else:
                column_map_list += [( col(f"{i.name}"), i.name)]
    

    return column_map_list


def string_to_json(raw_df, arg_input=[]):
    new_rdd = raw_df.rdd.map(lambda x : x[arg_input[0].strip()])
    return spark.read.json(new_rdd)



def data_flatten(raw_df, arg_input=[]):
    from common_utils import read_write_utils
    from pyspark.sql.functions import col

    while True:
        col_list = []
        explode_flag = True
        col_list = expand(raw_df.schema,'', [])
        new_list = []
        for i in col_list:
            if isinstance(i[1],tuple) and explode_flag:
                explode_flag = False
                new_list += [(i[0], (i[1][0].replace('~', '_'), i[1][1].replace('~', '_')))]
            elif isinstance(i[1],tuple) and not explode_flag:
                new_list += [(col(i[1][1].replace('~', '.')),i[1][1].replace('~', '_'))]
            else:
                new_list += [(i[0], i[1].replace('~', '_'))]

        raw_df = raw_df.select([col_ref.alias(*alias_ref) if isinstance(alias_ref,tuple) else col_ref.alias(alias_ref) for col_ref,alias_ref in  new_list])
        
        complex_obj =  [i for i in raw_df.schema if isinstance(i.dataType,pyspark.sql.types.StructType) or isinstance(i.dataType,pyspark.sql.types.ArrayType) ] 
        if not complex_obj :
            break

    #checking and performing column mapping
    if arg_input!= [] :  
        for oldname, newname in arg_input[0].items():
            raw_df = raw_df.withColumnRenamed(oldname, newname)
    
    return raw_df


def struct_expand( data_type, col_nm = '', column_map_list = []):
    import pyspark
    from pyspark.sql.functions import posexplode_outer, col
    for i in data_type:
        if isinstance(i.dataType,pyspark.sql.types.StructType):
            sec_nm = col_nm
            if col_nm != '':
                sec_nm = col_nm + '~'
            column_map_list += struct_expand( i.dataType, sec_nm + i.name, [])
        else :
            if col_nm != '':
                column_map_list += [( col(f"{col_nm.replace('~','.')}.{i.name}"), (col_nm + '~'+ i.name))]
            else:
                column_map_list += [( col(f"{i.name}"), i.name)]
    

    return column_map_list


def struct_flatten(raw_df, arg_input=[]):
    from common_utils import read_write_utils
    from pyspark.sql.functions import col

    while True:
        col_list = []
        explode_flag = True
        col_list = struct_expand(raw_df.schema,'', [])
        new_list = []
        for i in col_list:
            if isinstance(i[1],tuple) and explode_flag:
                explode_flag = False
                new_list += [(i[0], (i[1][0].replace('~', '_'), i[1][1].replace('~', '_')))]
            elif isinstance(i[1],tuple) and not explode_flag:
                new_list += [(col(i[1][1].replace('~', '.')),i[1][1].replace('~', '_'))]
            else:
                new_list += [(i[0], i[1].replace('~', '_'))]

        raw_df = raw_df.select([col_ref.alias(*alias_ref) if isinstance(alias_ref,tuple) else col_ref.alias(alias_ref) for col_ref,alias_ref in  new_list])
        
        complex_obj =  [i for i in raw_df.schema if isinstance(i.dataType,pyspark.sql.types.StructType)] 
        if not complex_obj :
            break

    #checking and performing column mapping
    if arg_input!= [] :  
        for oldname, newname in arg_input[0].items():
            raw_df = raw_df.withColumnRenamed(oldname, newname)
    
    return raw_df

def scd2_transformation(src_df, arg_input=[]):
    """
    column_list (in form of dict)
    Input :  [{"key_columns":["id"],
      "order_columns":["created_timestamp","updated_timestamp"],
      "start_tmsp_column_nm":"start_timestamp",
      "end_tmsp_column_nm" :"end_timestamp",
      "start_tmsp_column_map" : "created_timestamp",
      "target_name" : "master_table"
      }]

    """
    target_tbl = arg_input[0]['target_name']
    src_df = sort_and_rank(src_df, arg_input)
    stage_name = f'{target_tbl}_stage'.split('.')[-1]
    src_df.createOrReplaceTempView(stage_name)
    merge_sql = get_target_update(stage_name, arg_input, scd_type='scd2')
    print(merge_sql)
    try:
        tgt_df = spark.sql(f"select * from {target_tbl}")
        spark.sql(merge_sql)
        src_df = src_df.select(*tgt_df.columns)
    except:
        pass

    src_df = src_df.drop('least_rnum')

    return src_df



def scd1_transformation(src_df, arg_input=[]):
    """
    column_list (in form of dict)
    Input :  [{"key_columns":["id"],
      "order_columns":["created_timestamp","updated_timestamp"],
      "start_tmsp_column_nm":"start_timestamp",
      "end_tmsp_column_nm" :"end_timestamp",
      "start_tmsp_column_map" : "created_timestamp",
      "target_name" : "master_table"
      }]

    """
    from datetime import datetime

    target_tbl = arg_input[0]['target_name']


    src_df = sort_and_rank(src_df, arg_input, "scd1")

    src_df = src_df.where("active_ind = 'Y'").drop("active_ind")
    stage_name = f'{target_tbl}_stage'.split('.')[-1]
    src_df.createOrReplaceTempView(stage_name)
    merge_sql = get_target_update(stage_name, arg_input, scd_type='scd1')

    try:
        tgt_df = spark.sql(f"select * from {target_tbl}")
        print(merge_sql)
        spark.sql(merge_sql)
        src_df = src_df.select(*tgt_df.columns)
    except:
        pass

    src_df = src_df.drop('least_rnum')
    return src_df


def allow_new_inserts(src_df, arg_input=[]):
    from pyspark.sql.functions import col, when
    table_name = arg_input[0]['table_name']
    tbl_nm = table_name.split(".")[-1]
    print("target_table:", tbl_nm)
    col_maps = arg_input[0]['column_maps']
    combined_cond = None
    first_tgt = ""
    for src_col, tgt_col in col_maps.items():
        cond = ( col(f"src.{src_col}") == col(f"tgt.{tgt_col}") )
        if combined_cond != None:
            combined_cond = combined_cond & cond
        else:
            first_tgt = tgt_col
            combined_cond = combined_cond

    try:
        target_df = spark.table(table_name)
        joined_df = src_df.alias("src").join(target_df.alias("tgt"), combined_cond, "left_anti")
        result_df = joined_df.select("src.*")
    except Exception as e:
        result_df = src_df 


    

    return result_df

