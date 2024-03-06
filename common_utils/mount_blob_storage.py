from pyspark.sql import SparkSession 
spark= SparkSession.builder.getOrCreate()
from common_utils.utils import get_dbutils

dbutils = get_dbutils(spark)

dbutils.widgets.text("containerName", "")
dbutils.widgets.text("storageAccountName", "")
dbutils.widgets.text("sas", "")
dbutils.widgets.text("mount_name", "")
dbutils.widgets.text("source_name", "")

# COMMAND ----------

# DBTITLE 1,Mount Blob Storage into Databricks DBFS
def databricks_azure_blob_mount():
    """
    Mount azure blob storage container to databricks
	
	Parameters
	----------
	
	Return
    ------
	None 
	
	"""
    try:
        mount_name = dbutils.widgets.get("mount_name")
        container_name = dbutils.widgets.get("containerName")
        storage_account_name = dbutils.widgets.get("storageAccountName")
        sas = dbutils.widgets.get("sas")
        source_name = dbutils.widgets.get("source_name")
        
        if any(mount.mountPoint == mount_name for mount in dbutils.fs.mounts()):
            print("Directory is Already Mounted")
        else:
            config = "fs.azure.sas." + container_name+ "." + storage_account_name + ".blob.core.windows.net"
            dbutils.fs.mount(
              source = source_name,
              mount_point = mount_name,
              extra_configs = {config:sas})
    except Exception as e:
        print(f"Error while mounting blob storage- {e}")

databricks_azure_blob_mount()
