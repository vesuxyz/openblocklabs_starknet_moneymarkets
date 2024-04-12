import awswrangler as wr
import logging

S3_OUTPUT = "s3://aws-athena-query-results-us-east-1-522495932155/"

def query_athena(query: str, database: str):
    try:
        df = wr.athena.read_sql_query(sql=query, database=database, ctas_approach=False, s3_output=S3_OUTPUT)
        logging.info(
            f"Successfully ran Athena query:\n{query}"
        )
    except Exception as e:
        raise Exception(
            f"An error occurred while running Athena query:\n{query}\n\n{e}"
        )
    return df

def get_athena_prices_hourly(next_date):
    query = f"""
    SELECT * 
    FROM sui.token_prices_hourly 
    WHERE symbol IN ('ETH', 'USDC', 'USDT', 'DAI')
    AND timestamp = TIMESTAMP '{next_date}'
    """
    return query_athena(query, "sui")

def get_athena_uno_prices_hourly():
    query = """
    SELECT * FROM "AwsDataCatalog"."starknet"."uno_prices_hourly"
    """
    return query_athena(query, "starknet")