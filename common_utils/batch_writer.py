from common_utils import utils as utils,  read_write_utils

def batch_writer(batch_df, ingress_config, spark=None, mode="append"):
    _=spark
    
    batch_df =  read_write_utils.get_partition_df(ingress_config, batch_df)

    batch_df = batch_df.write

    kwargs = ingress_config["target"]["options"]
    batch_df = read_write_utils.apply_to_dataframe(batch_df, kwargs)

    read_write_utils.check_for_delta(ingress_config, batch_df)
