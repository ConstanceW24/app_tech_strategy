from pyspark.sql import SparkSession 
from common_utils.utils import get_dbutils
spark= SparkSession.builder.getOrCreate()

dbutils = get_dbutils(spark)

access_key = dbutils.widgets.get("access_key")
secret_key = dbutils.widgets.get("secret_key")
bucket_name = dbutils.widgets.get("bucket_name")
mount_name = dbutils.widgets.get("mount_name")


# DBTITLE 1,Show Existing Mount Points
def shows3mountpoints():
    print(dbutils.fs.mounts())
 
def mounts3bucket(access_key,secret_key,bucket_name,mount_name):
    """
    Mount S3 storage to databricks
	
	Parameters
	----------
    access_key
    secret_key
    bucket_name
    mount_name
	
	Return
    ------
	None 
	
	"""
    encoded_secret_key = secret_key.replace("/", "%2F")

    dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, bucket_name), "/mnt/%s" % mount_name)
    print(dbutils.fs.ls("/mnt/%s" % mount_name))

def unmounts3bucket(bucket_name):
    dbutils.fs.unmount("/mnt/"+bucket_name)


#calling mount s3 fucntion
mounts3bucket(access_key,secret_key,bucket_name,mount_name)

