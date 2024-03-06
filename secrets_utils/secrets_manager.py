import sys
import boto3
import json
import os
import base64
from botocore.exceptions import ClientError

def base64_decode(encoded_str):
    base64_bytes = encoded_str.encode("utf-8")
    sample_string_bytes = base64.b64decode(base64_bytes)
    sample_string = sample_string_bytes.decode("utf-8")
    return sample_string

def get_secrets():
    """
    This function is used to retrieve secrets from the aws secret manager.
    """

    get_params={
    'region' : "us-east-1"
       }

    get_params['access_key']=base64_decode(os.environ['ACCESS_KEY'])
    get_params['secret_key']=base64_decode(os.environ['SECRET_KEY'])
    get_params['env']=os.environ['ENV']

    secret_name = get_params['env']+'_gw_secrets'
    secret_dict=get_secretmgr_secrets(get_params,secret_name,'')
    return secret_dict

def get_secrets_param(secret_identifier):
    """
    This function is used to retrieve secrets from the aws secret manager.
    """

    get_params={
    'region' : "us-east-1"
       }

    get_params['access_key']=base64_decode(os.environ['ACCESS_KEY'])
    get_params['secret_key']=base64_decode(os.environ['SECRET_KEY'])
    get_params['env']=os.environ['ENV']

    secret_name = get_params['env']+'_' + secret_identifier + '_secrets'
    secret_dict=get_secretmgr_secrets(get_params,secret_name,secret_identifier)
    return secret_dict

def get_secretmgr_secrets(get_params,secret_name,secret_identifier):

    # Create a Secrets Manager client
    session = boto3.session.Session(
        aws_access_key_id = get_params['access_key'],
        aws_secret_access_key = get_params['secret_key']
    )
    client = session.client(
        service_name = 'secretsmanager',
        region_name = get_params['region']
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
            if secret_identifier in ['fsm', 'ers']:
                print("Exception encountered as ",e)
            else:
                print("Exception encountered as ",e)
                sys.exit(1)
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            #print(secret)
            return json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret