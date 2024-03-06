from azure.communication.email import EmailClient
from pyspark.sql import SparkSession
from common_utils import utils

spark= SparkSession.builder.getOrCreate()
dbutils = utils.get_dbutils(spark)

def send_email(subject,html_msg,recipients_addresses):
    """
    This function is used to send out an email based on the respective parameters.
    It makes use of azure communication service (ACS).
    Parameters
    ----------
    recipients_addresses : example : [{"address": "email address", "displayName": "display name"}]
    subject : email subject
    html_msg : email body
    """
    try:
        sender_address = dbutils.secrets.get(scope = "gw-cda-datacloud-akv-secrets", key = "gw-cda-acs-email-sender-address")
        connection_string = dbutils.secrets.get(scope = "gw-cda-datacloud-akv-secrets", key = "gw-cda-acs-connection-string")
        message = {
        "content": {
            "subject": subject,
            "plainText": "",
            "html": html_msg
        },
        "recipients": {
            "to": recipients_addresses
        },
        "senderAddress": sender_address
        }
        email_client = EmailClient.from_connection_string(connection_string)
        poller = email_client.begin_send(message)
        time_elapsed = 0
        poller_wait_time = 10

        while not poller.done():
            print("Email send poller status: " + poller.status())
            poller.wait(poller_wait_time)
            time_elapsed += poller_wait_time
            if time_elapsed > 18 * poller_wait_time:
                raise RuntimeError("Polling timed out.")
        
        if poller.result()["status"] == "Succeeded":
            print(f"Successfully sent the email (operation id: {poller.result()['id']})")
        else:
            raise RuntimeError(str(poller.result()["error"]))

    except Exception as ex:
        print(f"Encountered exception while sending an email: {ex}")