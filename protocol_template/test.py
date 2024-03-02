from dotenv import load_dotenv
import os
import sys
sys.path.append(".")

from utils.data import *
from protocol_template.function import *

# Configure database connections
load_dotenv()

snowflake_credentials = {
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "role": os.getenv('SNOWFLAKE_ROLE'),
    "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
}

conn = create_snowflake_connection(snowflake_credentials)

# Load SQL Query
with open(os.path.join('protocol_template', 'query.sql'), 'r') as file:
    query = file.read()

if __name__=='__main__':
    data = send_query(
        snowflake_connection=conn,
        query=query
    )
    # result = your_function(data)

