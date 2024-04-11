import logging
import json
import boto3


def get_secrets_manager_client():
    return boto3.session.Session(region_name='us-east-1').client(service_name='secretsmanager')


class GetSecretWrapper:    
    def __init__(self, secretsmanager_client: boto3.client):
        self.client = secretsmanager_client


    def get_secret(self, secret_name):
        """
        Retrieve individual secrets from AWS Secrets Manager using the get_secret_value API.

        :param secret_name: The name of the secret fetched.
        :type secret_name: str
        """
        try:
            get_secret_value_response = self.client.get_secret_value(
                SecretId=secret_name
            )
            logging.info("Secret retrieved successfully.")
            return json.loads(get_secret_value_response["SecretString"])
        except self.client.exceptions.ResourceNotFoundException:
            msg = f"The requested secret {secret_name} was not found."
            logging.info(msg)
            return msg
        except Exception as e:
            logging.error(f"An unknown error occurred: {str(e)}.")
            raise


# Example Usage

# secret_name = "obl/dune_api_key"

# session = boto3.session.Session()
# client = session.client(service_name='secretsmanager')
# key = GetSecretWrapper(client).get_secret(secret_name=secret_name)["dune_api_key"]
