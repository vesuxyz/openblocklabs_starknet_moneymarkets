from dotenv import load_dotenv
import os

from utils.data import *
# from protocol_example.function import custom_function

# Configure database connections
load_dotenv('.env')

snowflake_credentials = {
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "role": os.getenv('SNOWFLAKE_ROLE'),
    "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
}

# Load query
with open(os.path.join('protocol_example', 'query.sql'), 'r') as file:
    query = file.read()

# Execute script
if __name__=='__main__':
    conn = create_snowflake_connection(snowflake_credentials)
    data = send_query(
        snowflake_connection=conn,
        query=query
    )

    # result = custom_function(data)


