import datetime
import traceback
import json, requests
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from common_utils import utils as utils,  read_write_utils

spark= SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

def api_write(stream_df, egress_config, process_name = "Persistence"):

    def substitute_body(template, data):
        import json
        if template != "" :
            if not isinstance(template, str):
                new_body = ("{" + str(template) + "}").format(**data)
            else:
                new_body =  ("{" + template + "}").format(**data)
            return {} if new_body == '{}' else new_body
        else:
            return json.dumps(data) if isinstance(data,dict) else data
    
    def substitute_url(url, data):
        import json
        return url.format(**data)

    def header_dict(item_list):
        headers = {}
        for i in item_list:
            headers[i.strip()] = ""
        
        return headers
    
    def apply_authorization(headers):
        authorization_dict = egress_config["target"]["options"].get("tokenAuthorization",{})

        if authorization_dict != {} :
            auth_url , auth_header, auth_body, token_key = authorization_dict['url'], authorization_dict['headers'], authorization_dict['body'], authorization_dict.get("token_key", "access_token")
            response = requests.post(auth_url, headers=auth_header, data=auth_body)
            access_token = response.json()[token_key]
            if 'Authorization' in headers.keys() and token_key in headers['Authorization']:
                headers['Authorization'] = headers['Authorization'].replace(f"{token_key}", access_token)
            else:
                headers['Authorization'] = f'Bearer {access_token}'
        
        return headers   
    


    def make_requests(url, headers, body_template, payload, method = "post", pagination = {}):

        from datetime import datetime
        has_next_page = True  
        full_response = []
        success_response = []
        current_page = 0
        try:
            while has_next_page:  
                current_page = current_page + 1
                for i in range(2):
                    request_time = datetime.now()

                    payload_body = substitute_body(body_template, json.loads(payload))

                    if method == 'post':
                        updated_url = substitute_url(url, json.loads(payload))
                        response = requests.post(updated_url, data=payload_body, headers=headers) 
                    else:
                        updated_url = substitute_url(url, json.loads(payload))
                        response = requests.get(updated_url, data=payload_body, headers=headers) 

                    response_time = datetime.now()
                    print(response.status_code)
                    if str(response.status_code)[:2] == '20' :
                        break
                    elif str(response.status_code) == '500':
                        print(response.text)
                        headers = apply_authorization(headers)
                    else:
                        print(updated_url)
                        print(payload_body)
                        print(response.text)
                
                full_response = full_response + [(process_name, dataset_name,  request_time, current_page, json.dumps(headers), updated_url, json.dumps(payload_body), 
                                                response_time, response.status_code, 
                                                json.dumps(dict(response.headers)),
                                                json.dumps(response.json()) if str(response.status_code) in ('200','201','202') else '{}',
                                                response.text if str(response.status_code) not in  ('200','201','202') else '')]                
                
                if str(response.status_code) in ('200','201','202'):
                    # Process the data for the current page 
                    
                    if pagination == None or pagination == {} :
                        has_next_page = False
                    
                    else :
                        # pagination needs  tagname (header/body), key for current page, key for total page , substitute in headers/payload
                        if 'header' in pagination.get('section','').lower() :
                            # Check if there are more pages  
                            current_page_key = pagination['currentPageKey']
                            total_page_key = pagination['totalPageKey']
                            
                            if current_page_key in response.headers:  
                                current_page = int(response.headers[current_page_key])  
                                page_count = int(response.headers[total_page_key])  
                                has_next_page = current_page < page_count  
                                headers[current_page_key] = str(current_page + 1)

                            else:  
                                has_next_page = False
                        elif 'body' in pagination.get('section','').lower() :
                            # Check if there are more pages  
                            current_page_key = pagination['currentPageKey']
                            total_page_key = pagination['totalPageKey']
                            
                            if current_page_key in response.json():  
                                current_page = int(response.json()[current_page_key])  
                                page_count = int(response.json()[total_page_key])  
                                has_next_page = current_page < page_count  
                                headers[current_page_key] = str(current_page + 1)
                            else:  
                                has_next_page = False
                    success_response = success_response + [ (request_time, current_page, url, payload_body,  response.json())]
                else:
                    has_next_page = False
        except Exception as e:
            print(response.text)
            raise Exception(e)
        return full_response, success_response
    


    def batch_df_to_api(batch_df, _):
        from pyspark.sql.functions import lit

        headers = {  
        "Content-Type": "application/json"
        }

        body_template = egress_config["target"]["options"].get("payloadStructure","")

        # record = 1 call per record/ dataset = 1 call per Dataset
        data_to_call = egress_config["target"]["options"].get("postLevel","record").lower() 
        call_method = egress_config["target"]["options"].get("invokeMethod","post").lower()
        pagination_dict = egress_config["target"]["options"].get("pagination",{})

        source_file_name = egress_config["data"]["inputFile"]["fileName"].replace("/","")
        url = egress_config["target"]["options"]["url"]

        static_header = egress_config["target"]["options"].get("headers",{})
        headers.update(static_header)

        # Dynamic Authorization through token

        headers = apply_authorization(headers)

        dynamic_header = header_dict(egress_config["target"]["options"].get("dynamicHeaders",[]))
        if egress_config.get("cip_header",{}) != {}:
            dynamic_header = utils.apply_request_header(dynamic_header, egress_config["cip_header"])
        headers.update(dynamic_header)
        
        print(headers)

        response_recs = []
        success_resp = []
        pandas_df = batch_df.select(F.to_json(F.struct("*")).alias("values")).toPandas()
        data_payload = []
        for payload in pandas_df['values']:
            if data_to_call == "record":
                
                response_list, success_list = make_requests(url, headers, body_template, payload, method = call_method, pagination = pagination_dict)
                response_recs += response_list
                success_resp += success_list
            else:
                data_payload = data_payload + [json.loads(payload)]

        if data_to_call != "record" and data_payload != []:
            response_list, success_list = make_requests(url, headers, body_template, payload, method = call_method, pagination = pagination_dict)
            response_recs += response_list
            success_resp += success_list
        
        if response_recs != [] and egress_config["target"]["options"].get("logging", {}) != {}:
            response_df = spark.createDataFrame(response_recs, ["process_name","dataset_name",  "request_time", "invoking_page", "request_headers", "request_url", "request_payload", "response_time",  "response_status_code", 
                                         "response_headers",
                                         "response_body",
                                         "response_error"])
            stream_df = response_df.write.mode(egress_config["target"]["options"]["logging"].get("mode", "append")).format(egress_config["target"]["options"]["logging"]['format'])
            
            if str(egress_config["target"]["options"]["logging"].get("table", "")).lower() not in ( "", "none"):
                stream_final_df = stream_df\
                    .option("path",egress_config["target"]["options"]["logging"]['path'])\
                    .saveAsTable(egress_config["target"]["options"]["logging"]['table'])
            else:
                stream_final_df = stream_df.save(egress_config["target"]["options"]["logging"]['path'])

        if success_resp != [] :
            import pandas as pd
            response_pdf = pd.DataFrame(success_resp, columns=["request_time", "invoking_page", "request_url", "request_body" , "response_body"])
            response_df = spark.createDataFrame(response_pdf)
            print(response_df.dtypes)


            stream_df = response_df.write.mode(egress_config["target"]["options"].get("mode", "append")).format(egress_config["target"]["options"]['format'])
            
            if str(egress_config["target"]["options"].get("table", "")).lower() not in ( "", "none"):
                stream_final_df = stream_df\
                    .option("path",egress_config["target"]["options"]['path'])\
                    .saveAsTable(egress_config["target"]["options"]['table'])
            else:
                stream_final_df = stream_df.save(egress_config["target"]["options"]['path'])
    
    # Batch Mode
                
    dataset_name = egress_config["data"]["eventTypeId"]
    if str(egress_config["data"]["inputFile"].get('batchFlag','none')).lower() == 'true':
        batch_df_to_api(stream_df, int(0))

    # Stream Mode
    else:

        stream_df = stream_df.writeStream.foreachBatch(batch_df_to_api)
        stream_df = read_write_utils.apply_trigger_option(egress_config, stream_df)
        stream_df = stream_df.outputMode("append").option("checkpointLocation", egress_config["target"]["options"]["checkpointLocation"])\
                    .queryName(process_name)
        
        stream_final_df = stream_df.start()

        read_write_utils.check_target_trigger(egress_config, stream_final_df)   