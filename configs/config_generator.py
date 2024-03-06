import copy

ingestion_config_template = {
  "configId":"",
  "configuration": {
    "data": {
      "inputFile": {
        "schema": "NA",
        "options": {
          "mergeSchema": "true",
          "maxFilesPerTrigger": "100",
          "ignoreChanges": "true"
        },
        "fileName": "__table_name__/",
        "inputFileFormat": "__format__",
        "batchFlag": "true"
      },
      "eventTypeId": "__table_name__"
    },
    "rules": [],
    "miscellaneous": [],
    "source": {
      "driver": {
        "path": "",
        "format": "",
        "SourceType": "",
        "checkpointLocation": ""
      },
      "dependents": []
    },
    "target": {
      "options": {
        "mergeSchema": "true",
        "overwriteSchema": "true",
        "checkpointLocation": ""
      },
      "targetType": "s3",
      "noofpartition": "1",
      "targetTrigger": "once"
    }
  }
}

def get_config(src_dict, tgt_dict, node="DataIngestionNode"):
    
    if node == "DataIngestionNode" :
        ingestion_config = ingestion_config_template
        ingestion_config['configuration']['source']['driver']['path'] = src_dict['path'].strip()
        ingestion_config['configuration']['source']['driver']['SourceType'] = src_dict['source-type']
        ingestion_config['configuration']['source']['driver']['format'] = src_dict['format']
        ingestion_config['configuration']['data']['inputFile']['inputFileFormat'] = src_dict['format']
        ingestion_config['configuration']['data']['inputFile']['fileName'] = src_dict['path'].strip().split('/')[-1]
        ingestion_config['configuration']['data']['eventTypeId'] = ingestion_config['configuration']['data']['inputFile']['fileName']
        ingestion_config['configId'] = ingestion_config['configuration']['data']['inputFile']['fileName']


        del src_dict['path']
        del src_dict['format']
        del src_dict['source-type']

        ingestion_config['configuration']['data']['inputFile']['options'].update(src_dict)

        ingestion_config['configuration']['target']['targetType'] = tgt_dict['target-type']
        del tgt_dict['target-type']

        ingestion_config['configuration']['target']['options'].update(tgt_dict)

        print(ingestion_config)

        return ingestion_config

