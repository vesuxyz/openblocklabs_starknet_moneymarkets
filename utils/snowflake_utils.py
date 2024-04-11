from snowflake.connector import SnowflakeConnection, connect
import boto3
import pandas as pd

from utils.secret_manager import GetSecretWrapper


def get_snowflake_credentials() -> dict:
    """
    Fetch default credentials for Snowflake warehouse from AWS Secrets Manager.

    :return snowflake_credentials: dictionary containing warehouse connection credentials
    :type snowflake_credentials: dict
    """

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    snowflake_credentials = GetSecretWrapper(client).get_secret(
        secret_name="snowflake/airflow/credentials"
    )

    return snowflake_credentials

def create_snowflake_connection(
        credentials: dict = None
) -> SnowflakeConnection:
    """
    Creates connection to a Snowflake warehouse.

    Credentials should be a dict with keys:
    - "user"
    - "password"
    - "account"
    - "role"
    - "warehouse"

    If no credentials are provided, default credentials are pulled from AWS secrets manager.

    :param credentials: dictionary containing Snowflake credentials
    :type credentials: dict
    :return connect: Snowflake connection object
    :type connect: SnowflakeConnection
    """

    if credentials is None:
        credentials = get_snowflake_credentials()

    return connect(
        user=credentials["user"],
        password=credentials["password"],
        account=credentials["account"],
        role=credentials["role"],
        warehouse=credentials["warehouse"],
    )


def query_snowflake(snowflake_connection: SnowflakeConnection, query: str) -> pd.DataFrame:
    """
    Executes a query to a Snowflake connection and returns as a pandas DataFrame.

    :param snowflake_connection: connection to Snowflake warehouse
    :type snowflake_connection: SnowflakeConnection
    :param query: SQL query to execute
    :type query: str
    :return df: query results from Snowflake
    :type df: pandas DataFrame
    """

    cursor = snowflake_connection.cursor()
    result = cursor.execute(query)
    df = result.fetch_pandas_all()

    return df

def get_snowflake_strk_prices_hourly():
    query = """
    SELECT DISTINCT
        'STRK' AS symbol,
        timestamp,
        price
    FROM OBL_STARKNET_DW.STARKNET.STRK_PRICES_HOURLY
    """
    return query_snowflake(query)