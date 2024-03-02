from snowflake.connector import SnowflakeConnection, connect
import pandas as pd

def create_snowflake_connection(
    credentials,
) -> SnowflakeConnection:
    return connect(
        user=credentials["user"],
        password=credentials["password"],
        account=credentials["account"],
        role=credentials["role"],
        warehouse=credentials["warehouse"],
    )


def send_query(
    snowflake_connection: SnowflakeConnection,
    query: str,
) -> pd.DataFrame:

    cursor = snowflake_connection.cursor()
    result = cursor.execute(query)
    df = result.fetch_pandas_all()
    return df