from azure.keyvault import KeyVaultClient, KeyVaultAuthentication
from azure.common.credentials import ServicePrincipalCredentials

credentials = None

def auth_callback(server, resource, scope):
    """
    auth_callback

    This Function is used to authenticate the keyvault with the respective azure cleint id,secret,tenant id.
    Parameters:
        -server
        -resource
        -scope
    Return:
        It returns keyvault access token to access stored secrets.

    """
    credentials = ServicePrincipalCredentials(
        client_id = '',
        secret = '',
        tenant = '',
        resource = "https://vault.azure.net"
    )
    token = credentials.token
    return token['token_type'], token['access_token']


def get_keyvault_secrets(secret_key):
    """
    get_keyvault_secrets

    This Function is used to get the secrets from key vault based on the secret key passed.
    Parameters:
        - secret key
    Return:
        It returns the secret value.

    """
    client = KeyVaultClient(KeyVaultAuthentication(auth_callback))
    secret_bundle = client.get_secret("https://vault_url", secret_key)
    return secret_bundle.value