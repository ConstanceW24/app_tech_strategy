
# Industry Cloud Data Pipeline

## Features:

 Industry Cloud Data Pipeline is a streaming data platform capable of processing of data events by spectrum of services, guided by Data Initialization Catalog of Control plane and powered by business rules defined in configurations.

![DP](https://github.pwc.com/storage/user/11775/files/d9acf6f1-1cd3-4fd0-9d14-a5842d12f9fa)


## Architecture & Design Principles

1.	Streaming First Architecture: Data is a stream of events that needs to be processed in real time following eventing paradigm
2.	Event Journey: Every Data Event (from data stream) has a prescribed journey which is enabled by metadata driven data pipelines.
3.	Continuous Data Pipelines: Data is a stream of events which requires tailored activation and processing in real time. Continuous Data Pipelines composed of independent loosely coupled atomic data services, acts on events based on predefined configurations without the need to create custom code for each event. 
4.	Event Paradigm: Event-based mechanism to provision and execute a dynamic Data Pipeline based on Initialization and configurations defined for the Event. The Event identifies the data services applicable (from service catalog) and defines Initialization (in Initialization catalog) to activate on arrival in Platform.
5.	Service To Service Communication: Service to service communication should occur via topic-based model to lend higher scalability, fault tolerance and performance to data services.
6.	Metadata Driven Processing: Data Services powered by metadata and configurations to process and transform data, agnostic of the data. Abstracting and decoupling of creation and application of business rules. Catalogs, stores the configuration for events and Data services understands the application of business rules, thereby driving the metadata-based processing.
7.	Data Initialization: Dynamic pipelines defined by Data Initialization, identifies the data services and order to process the event.
8.	Atomic Data Services: Focused data services driven by single responsibility principle to perform single task on an event. Data services follow a topic-based consumption model to ingress, act and egress the event. Single service shared across data Initializations.
9.	Data Lineage (FSM): State changes of data in Data Pipeline will be captured in FSM for audit regulatory and compliance requirements.
10.	Data Controls: Event schema control and governance mechanism driven by Metadata Catalog to validate and enforce data schema.
11.	Data Governance: Data level access control mechanism powered by data policies (stored in Policy/Consent Engine) to control access to data

## Dependencies

* Configurations should be availble in the control plane during the design time

## Prerequisites

* Data Plane Event needs to be registered in DP Event Registry   
  Request:

```
   {
    "eventTypeId": "partyperson",
    "id": "b1b23f84-748b-4ef9-9124-c08abb37dc84",
    "eventType": "partyperson",
    "description": "string",
    "schemaId": "a9a094a441374aabbd6ba398b7ff50ee",
    "version": 1,
    "status": "ACTIVE",
    "createdDate": "2022-11-28T04:21:32.965+00:00",
    "createdBy": "string",
    "updatedDate": "2022-11-28T04:21:32.965+00:00",
    "updatedBy": "string"
  },

```

## Services Invoked
   Initialization process will retrieve the Initializations and the configurations from the respective stores API and will internalize it in the memory.With Initialization Service, we specify the DAG to process the event, data services required to process the event and execution order of the services.

* Data Ingestion
* Data Quality
* Data Transformation
* Data Persistence

Data Ingestion:

This service performs data transfer from source location to the target destination. It receives the ingestion config details as parameter from 
the Data_Initialization file, gets the respective source location 
and destination for each event.

Data Quality:

This service performs data quality checks on the event payload. The service has a pre-built catalog of data quality checks that 
can be performed on attributes in event payload. 

Data Transformation:

This service performs record level transformation on the event payload
It receives the data transformation config as parameter from the data_initialization file, 
iterates the rules list from the configuration and perform the field level transformations
and write the transformed/enriched data to DW/DB for next service.

Data Persistence:

This service performs persistence of the Transformed/Augmented event payload to egress Topic.
It writes the spark dataframe to the target location defined in the configuration.

## Steps to Use Industry Cloud Data Pipeline

### Step1:
          For a new File it would require different types of config files for differnet services such as initialization ,Ingestion, Quality,Transformation & Persistence.

### Step2: Prepare & Add below sample config files for all the services which will be pushed into Swagger(CP Service).

  #### Initialization config
      {
        "id": "b272891f-0caa-467a-a51a-0d72f487f8b5",
        "initializationId": "partyperson005",
        "name": "partyperson Details",
        "description": "Initialization to process partyperson Event",
        "initializationType": "Banking",
        "eventTypeId": "policydata",
        "status": "ACTIVE",
        "nodes": [
          {
            "nodeId": "101",
            "nodeName": "DataIngestionNode",
            "description": "Data Ingestion Service",
            "eventTypeId": "policydata",
            "eventTypeName": "partyperson",
            "configuration": {
              "configId": "partypersondi005",
              "configName": "Ingestion_Config"
            }
          },
          {
            "nodeId": "102",
            "nodeName": "DataQualityNode",
            "description": "Data Quality Service",
            "eventTypeId": "policydata",
            "eventTypeName": "partyperson",
            "configuration": {
              "configId": "partypersondq005",
              "configName": "Quality_Config"
            }
          },
          {
            "nodeId": "103",
            "nodeName": "DataTransformationNode",
            "description": "Data Transformation Service",
            "eventTypeId": "policydata",
            "eventTypeName": "partyperson",
            "configuration": {
              "configId": "partypersondt005",
              "configName": "Transformation_Config"
            }
          },
          {
            "nodeId": "105",
            "nodeName": "DataPersistance",
            "description": "Data PersistanceService",
            "eventTypeId": "policydata",
            "eventTypeName": "partyperson",
            "configuration": {
              "configId": "partypersondp005",
              "configName": "Persistance_Config"
            }
          }
        ],
        "version": 1,
        "createdBy": "PwC Sector Cloud",
        "createdDate": "2022-11-21T07:41:31.378+00:00",
        "updatedBy": "PwC Sector Cloud",
        "updatedDate": "2022-11-21T07:41:31.378+00:00"
      }
  #### Data Ingestion config
      {
        "id": "604991b3-9627-4edb-a594-147b45d42f91",
        "configId": "partypersondi005",
        "name": "Data_Ingestion_partyperson_Config",
        "description": "Data Ingestion for partyperson File",
        "configType": "Ingestion_Config",
        "version": 5,
        "status": "ACTIVE",
        "configuration": {
          "data": {
            "inputFile": {
              "schema": "partyUniqueIdentifier string, prefixName string, firstName string, middleName string, middleInitial string, lastName string, suffixName string, maidenName string, informalName string,email string,phone string,address string, birthDate string, deceasedFlag string, citizenshipCountry string, crdbPartyIdentifier string, passportIssuingAuthority string, passportIssuingCountry string, passportNumber string, passportIssueDate string, passportExpirationDate string, socialSecurityNumber string, taxIdentifierNumber string, issuingAuthority string, issuingCountry string, issuingStateProvince string, licenseNumber string, issueDate string, expirationDate string",
              "options": {
                "sep": ",",
                "header": "true",
                "inferSchema": "false",
                "maxFilesPerTrigger": "1"
              },
              "fileName": "partyperson/",
              "inputFileFormat": "csv"
            },
            "eventTypeId": "partyperson"
          },
          "rules": [],
          "source": {
            "driver": {
              "path": "/mnt/ccsdp/ccs_dp_demo/rawdata/partyperson/",
              "format": "csv",
              "SourceType": "blob",
              "checkpointLocation": ""
            },
            "dependents": []
          },
          "status": "Active",
          "target": {
            "options": {
              "path": "/mnt/ccsdp/ccs_dp_demo/dataplane/data_ingestion/partyperson/",
              "format": "csv",
              "header": "true",
              "checkpointLocation": "/mnt/ccsdp/ccs_dp_demo/dataplane/checkpoint/data_ingestion/partyperson/"
            },
            "targetType": "blob",
            "noofpartition": "4",
            "targetTrigger": "once"
          }
        },
        "createdDate": "2022-11-21T07:39:26.048+00:00",
        "createdBy": "PwC Sector Cloud",
        "updatedDate": "2022-12-01T07:55:35.060+00:00",
        "updatedBy": "PwC Sector Cloud"
      }
  #### Data Quality config
      {
        "id": "1a6084da-2def-49fe-9da5-3fea2b98834e",
        "configId": "partypersondq005",
        "name": "Data_Quality_partyperson_Config",
        "description": "Data Quality Rules for partyperson File",
        "configType": "Data Quality",
        "version": 7,
        "status": "ACTIVE",
        "configuration": {
          "data": {
            "inputFile": {
              "schema": "partyUniqueIdentifier string, prefixName string, firstName string, middleName string, middleInitial string, lastName string, suffixName string, maidenName string, informalName string,email string,phone string,address string, birthDate string, deceasedFlag string, citizenshipCountry string, crdbPartyIdentifier string, passportIssuingAuthority string, passportIssuingCountry string, passportNumber string, passportIssueDate string, passportExpirationDate string, socialSecurityNumber string, taxIdentifierNumber string, issuingAuthority string, issuingCountry string, issuingStateProvince string, licenseNumber string, issueDate string, expirationDate string",
              "options": {
                "sep": ",",
                "header": "true",
                "inferSchema": "false",
                "maxFilesPerTrigger": "1"
              },
              "fileName": "partyperson/",
              "inputFileFormat": "csv"
            },
            "eventTypeId": "partyperson"
          },
          "rules": [
            {
              "rule_id": "1",
              "field_name": "expirationDate",
              "rule_parser": "PySpark",
              "default_value": "",
              "rule_override": "",
              "validation_name": "timestamp_validation",
              "validation_input": "yyyy-mm-dd HH:mm:ss",
              "exception_handling": "",
              "validation_output_field": "timestampValidation_expirationDate"
            },
            {
              "rule_id": "2",
              "field_name": "phone",
              "rule_parser": "PySpark",
              "default_value": "",
              "rule_override": "",
              "validation_name": "stringlength_validation",
              "validation_input": "12",
              "exception_handling": "",
              "validation_output_field": "StringLengthValidation_phone"
            },
            {
              "rule_id": "3",
              "field_name": "email",
              "rule_parser": "PySpark",
              "default_value": "",
              "rule_override": "",
              "validation_name": "specialcharacter_validation",
              "validation_input": "@",
              "exception_handling": "",
              "validation_output_field": "specialcharacterValidation_email"
            },
            {
              "rule_id": "4",
              "field_name": "lastName",
              "rule_parser": "PySpark",
              "default_value": "default_lastname",
              "rule_override": "",
              "validation_name": "specificvalue_validation",
              "validation_rule": "",
              "validation_input": "Johnson,Mahoney",
              "exception_handling": "Default",
              "validation_output_field": "specificvalueValidation_lastName"
            }
          ],
          "source": {
            "driver": {
              "path": "/mnt/ccsdp/ccs_dp_demo/dataplane/data_ingestion/partyperson/",
              "format": "csv",
              "header": "true",
              "SourceType": "blob",
              "checkpointLocation": ""
            },
            "dependents": []
          },
          "status": "Active",
          "target": {
            "options": {
              "path": "/mnt/ccsdp/ccs_dp_demo/dataplane/data_quality/partyperson/",
              "format": "csv",
              "header": "true",
              "checkpointLocation": "/mnt/ccsdp/ccs_dp_demo/dataplane/checkpoint/data_quality/partyperson/"
            },
            "targetType": "blob",
            "noofpartition": "4",
            "targetTrigger": "once"
          }
        },
        "createdDate": "2022-11-21T07:38:48.795+00:00",
        "createdBy": "PwC Sector Cloud",
        "updatedDate": "2022-12-01T07:56:12.439+00:00",
        "updatedBy": "PwC Sector Cloud"
      }

        
  #### Data Transformation config    
      {
        "id": "9d662f40-c821-452d-9206-47870ba7a73f",
        "configId": "partypersondt005",
        "name": "Data_Transformation_partyperson_Config",
        "description": "Data Transformation Rules for partyperson Event",
        "configType": "Transformation_config",
        "version": 5,
        "status": "ACTIVE",
        "configuration": {
          "data": {
            "inputFile": {
              "schema": "partyUniqueIdentifier string, prefixName string, firstName string, middleName string, middleInitial string, lastName string, suffixName string, maidenName string, informalName string,email string,phone string,address string, birthDate string, deceasedFlag string, citizenshipCountry string, crdbPartyIdentifier string, passportIssuingAuthority string, passportIssuingCountry string, passportNumber string, passportIssueDate string, passportExpirationDate string, socialSecurityNumber string, taxIdentifierNumber string, issuingAuthority string, issuingCountry string, issuingStateProvince string, licenseNumber string, issueDate string, expirationDate string",
              "options": {
                "sep": ",",
                "header": "true",
                "inferSchema": "false",
                "maxFilesPerTrigger": "1"
              },
              "fileName": "partyperson",
              "inputFileFormat": "csv"
            },
            "eventTypeId": "partyperson"
          },
          "rules": [
            {
              "rule_id": "1",
              "rule_parser": "SparkSQL",
              "rule_override": "select partyperson.*, partyemployment.employmentstatus,partyemployment.employmenttype,partyemployment.employername,partyemployment.isrestrictedindustryflag,partyemployment.issensitiveindustryflag,partyemployment.industry,partyemployment.subindustry,partyemployment.occupation,partyemployment.employermailingaddress,partyfinance.sourceofwealth,partyfinance.annualincomeusd,partyfinance.liquidnetworthamountusd,partyfinance.wealthnotinusaflag,partyfinance.wealthnotinusacountry from partyperson  LEFT JOIN partyemployment  ON partyperson.partyUniqueIdentifier=partyemployment.partyUniqueIdentifier LEFT JOIN partyfinance  ON partyperson.partyUniqueIdentifier=partyfinance.partyUniqueIdentifier",
              "Transformation_name": "Join_Transformation",
              "Transformation_input": [
                "partyperson.*, partyemployment.*,partyfinance.*"
              ],
              "Transformation_output": [
                "partyUniqueIdentifier,prefixName,firstName,middleName,middleInitial,lastName,suffixName,maidenName,informalName,email,phone,address,birthDate,,citizenshipCountry,crdbPartyIdentifier,passportIssuingAuthority,passportIssuingCountry,passportNumber,passportIssueDate,passportExpirationDate,socialSecurityNumber,taxIdentifierNumber,issuingAuthority,issuingCountry,issuingStateProvince,licenseNumber,issueDate,expirationDate,employmentStatus string,employmentType string,employerName string,isRestrictedIndustryFlag string,isSensitiveIndustryFlag string,industry string,subIndustry string, occupation string,employerMailingAddress string,sourceOfWealth string,annualIncomeUSD string,liquidNetWorthAmountUSD string,wealthNotInUSAFlag string,wealthNotInUSACountry string"
              ]
            }
          ],
          "source": {
            "driver": {
              "path": "/mnt/ccsdp/ccs_dp_demo/dataplane/data_quality/partyperson/",
              "format": "csv",
              "header": "true",
              "sourceType": "blob",
              "checkpointLocation": ""
            },
            "dependents": [
              {
                "path": "/mnt/ccsdp/ccs_dp_demo/enrichment/external/",
                "schema": "partyUniqueIdentifier string,employmentStatus string,employmentType string,employerName string,isRestrictedIndustryFlag string,isSensitiveIndustryFlag string,industry string,subIndustry string, occupation string,employerMailingAddress string",
                "options": {
                  "sep": ",",
                  "header": "true",
                  "inferSchema": "false"
                },
                "filename": "partyemployment",
                "SourceType": "blob",
                "inputFileFormat": "csv",
                "checkpointLocation": ""
              },
              {
                "path": "/mnt/ccsdp/ccs_dp_demo/enrichment/external/",
                "schema": "partyUniqueIdentifier string,sourceOfWealth string,annualIncomeUSD string,liquidNetWorthAmountUSD string,wealthNotInUSAFlag string,wealthNotInUSACountry string",
                "options": {
                  "sep": ",",
                  "header": "true",
                  "inferSchema": "false"
                },
                "filename": "partyfinance",
                "SourceType": "blob",
                "inputFileFormat": "csv",
                "checkpointLocation": ""
              }
            ]
          },
          "status": "Active",
          "target": {
            "options": {
              "path": "/mnt/ccsdp/ccs_dp_demo/dataplane/data_transformation/partyperson/",
              "format": "csv",
              "header": "true",
              "checkpointLocation": "/mnt/ccsdp/ccs_dp_demo/dataplane/checkpoint/data_transformation/partyperson/"
            },
            "targetType": "blob",
            "noofpartition": "4",
            "targetTrigger": "once"
          }
        },
        "createdDate": "2022-11-21T07:40:00.487+00:00",
        "createdBy": "PwC Sector Cloud",
        "updatedDate": "2022-12-01T07:57:11.636+00:00",
        "updatedBy": "PwC Sector Cloud"
      }
  #### Data Persistence config
      {
        "id": "ec286c9b-59a2-4149-9a54-a949377f0fe4",
        "configId": "partypersondp005",
        "name": "Data_Persistance_partyperson_Config",
        "description": "Data Persistance for partyperson File",
        "configType": "Data Persistance",
        "version": 5,
        "status": "ACTIVE",
        "configuration": {
          "data": {
            "inputFile": {
              "schema": "partyUniqueIdentifier  string,prefixName string,firstName string,middleName string,middleInitial string,lastName  string,suffixName string,maidenName string,informalName string,email string,phone string, address string, birthDate date,deceasedFlag string,citizenshipCountry string,crdbPartyIdentifier string,passportIssuingAuthority string,passportIssuingCountry string,passportNumber string,passportIssueDate date,passportExpirationDate date,socialSecurityNumber string,taxIdentifierNumber string,issuingAuthority string,issuingCountry string,issuingStateProvince string,licenseNumber string,issueDate date,expirationDate date,employmentStatus string,employmentType string,employerName string,isRestrictedIndustryFlag string,isSensitiveIndustryFlag string,industry string,subIndustry string,occupation string,employerMailingAddress string,sourceOfWealth string,annualIncomeUSD string,liquidNetWorthAmountUSD string,wealthNotInUSAFlag string,wealthNotInUSACountry string",
              "options": {
                "sep": ",",
                "header": "true",
                "inferSchema": "false"
              },
              "fileName": "partyperson/",
              "inputFileFormat": "csv"
            },
            "eventTypeId": "partyperson"
          },
          "rules": [],
          "source": {
            "driver": {
              "path": "/mnt/ccsdp/ccs_dp_demo/dataplane/data_transformation/partyperson/",
              "format": "csv",
              "SourceType": "blob",
              "checkpointLocation": ""
            },
            "dependents": []
          },
          "status": "Active",
          "target": {
            "options": {
              "path": "/mnt/ccsdp/ccs_dp_demo/dataplane/data_persistence/partyperson/",
              "format": "csv",
              "header": "true",
              "checkpointLocation": "/mnt/ccsdp/ccs_dp_demo/dataplane/checkpoint/data_persistence/partyperson/"
            },
            "targetType": "blob",
            "noofpartition": "4",
            "targetTrigger": "once"
          }
        },
        "createdDate": "2022-11-21T07:40:30.244+00:00",
        "createdBy": "PwC Sector Cloud",
        "updatedDate": "2022-12-01T07:49:23.432+00:00",
        "updatedBy": "PwC Sector Cloud"
      }
### Step3: In Databricks inside Workflow section we will create a Databricks Job using python wheel.

![job](https://github.pwc.com/storage/user/11775/files/104c8cfe-edbb-48c2-b160-e3b398bda328) 

### Step4: We will trigger the Databricks Job passing event_id & eventmapinstance_id as parameter.

![job](https://github.pwc.com/storage/user/11775/files/4affa67c-e966-42a2-865e-56227141043e)

### Step5: 
          Databricks Job will eventually trigger all the services in Data Pipeline(DI,DQ,DT,DP) and stores the data at
          the provided DB/DW.
          * Note: Target details should be provided in Data Persistence Configuration File 


Jars & Drivers may required based on the Source & Target

Avro:
Spark JAR: spark-avro

XML (eXtensible Markup Language):
Spark JAR: spark-xml

Excel
Spark JAR: spark-excel

cobol
Spark Jar : spark-cobol , cobol_parser, scodec_bits, scodec_core

JDBC (Java Database Connectivity):
Spark JAR: spark-sql

Hive tables:
Spark JAR: spark-hive

Cassandra:
Spark JAR: spark-cassandra-connector

Delta Tables:
Spark JAR: delta-core

Hudi (Apache Hudi):
Spark JAR: hudi-spark-bundle

Iceberg (Apache Iceberg):
Spark JAR: iceberg-spark3-runtime


Driver Jars
============
Amazon Redshift:
JDBC Driver: Amazon Redshift JDBC Driver
JAR: RedshiftJDBC4.jar

Google BigQuery:
JDBC Driver: Simba JDBC Driver for Google BigQuery
JAR: bigquery-connector.jar

Snowflake:
JDBC Driver: Snowflake JDBC Driver
JAR: snowflake-jdbc.jar

Microsoft Azure Synapse Analytics (formerly SQL Data Warehouse):
JDBC Driver: Microsoft JDBC Driver for SQL Server
JAR: mssql-jdbc.jar

IBM Db2 Warehouse:
JDBC Driver: IBM Data Server Driver for JDBC and SQLJ
JAR: db2jcc4.jar

Teradata:
JDBC Driver: Teradata JDBC Driver
JAR: terajdbc4.jar and tdgssconfig.jar

Oracle Database:
JDBC Driver: Oracle JDBC Driver
JAR: ojdbc8.jar (for Oracle 12c and above)

MySQL:
JDBC Driver: MySQL Connector/J
JAR: mysql-connector-java.jar

PostgreSQL:
JDBC Driver: PostgreSQL JDBC Driver
JAR: postgresql.jar

Microsoft SQL Server:
JDBC Driver: Microsoft JDBC Driver for SQL Server
JAR: mssql-jdbc.jar