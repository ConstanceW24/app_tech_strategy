import os

field_datatype="<class 'pyspark.sql.types.StructType'>"
varchartype="Varchar(10485760)"
numbertype="NUMERIC(38,0)"
decimaltype="NUMERIC"
env = os.environ['ENV']
table_list_file = "table_list.csv"
lob_map = {
    'policy_center' : 'pc',
    'claim_center' : 'cc',
    'contact_manager' : 'cm',
    'billing_center' : 'bc'
    }

param = {
    "dev":
    {
        "secret-scope" : "gw-cda-datacloud-akv-secrets",
        "cda" :{
            "secret-storage-account-name":"gw-cda-storage-account-name",
            "secret-storage-account-key":"gw-cda-storage-account-key",
            "secret-principal-client-id": "gw-cda-service-principle-client-id",
            "secret-principal-client-key": "gw-cda-service-principle-client-key",
            "secret-mount-path":"gw-cda-mnt-path-key",
            "secret-container-name" : "gw-cda-container-name",
            "secret-container-connection-string" : "gw-cda-blob-connection-string",
        },
        "s3" :{
            "secret-mount-path":"gw-cda-s3-mnt-path-key",
            "secret-s3-env":"gw-cda-s3-env",
            "secret-s3-access-key": "gw-cda-s3-access-key",
            "secret-s3-secret-key": "gw-cda-s3-secret-key",
            "secret-s3-bucket-name":"gw-cda-s3-bucket-name",
            "secret-s3-center-loadts" : "gw-cda-center-loadts"

        },
        "db" :
        {
            "synapse" :
            {
               "secret-host" : "gw-cda-azure-synapse-host",
               "secret-port" : "gw-cda-azure-synapse-port",
               "secret-database" : "gw-cda-azure-synapse-dbname",
               "username" : "",
			   "password" : "",
               "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
               "tempdir" : ""
            },
             "onprem" :
            {
               "secret-param-host" : "gw-cda-onprem-dev-sql-dbhost",
               "secret-param-port" : "gw-cda-onprem-dev-sql-dbport",
               "secret-param-database" : "gw-cda-onprem-dev-sql-dbname",
			   "secret-param-username" : "gw-cda-onprem-dev-sql-username",
			   "secret-param-password" : "gw-cda-onprem-dev-sql-pwd",
               "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
               "tempdir" : ""
            },
        },
        "email": {
            "from" : "",
            "to" :"",
            "cc" : "",
            "secret-param-connection" : "dp-acs-connection-string"
        }

    }

}


