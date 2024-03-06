def batch_writer(batch_df, ingress_config, spark=None, mode="append"):
    _=spark
    if "noofpartition" in ingress_config["target"].keys():
        batch_df = batch_df.coalesce(
            int(ingress_config["target"]["noofpartition"])
        ).write
    else:
        batch_df = batch_df.write

    kwargs = ingress_config["target"]["options"]
    if len(kwargs) > 0:
        for k, v in kwargs.items():
            if k == "format":
                batch_df = batch_df.format(v)
            elif k in ["path", "checkpointLocation", "table"]:
                continue
            elif k == "partitionBy":
                batch_df = batch_df.partitionBy(*(v.strip().split(",")))
            else:
                batch_df = batch_df.option(k, v)

    processing_engine = ingress_config.get("processingEngine", "")
    if (
        ingress_config["target"].get("options", "").get("format", "") == "delta"
        and processing_engine != "EMR"
    ):
        ## Write the dataframe to path
        batch_df = (
            batch_df.mode(mode)
            .option("path", ingress_config["target"]["options"]["path"])
            .saveAsTable(ingress_config["target"]["options"]["table"])
        )
    else:
        batch_df.mode(mode).save(ingress_config["target"]["options"]["path"])
