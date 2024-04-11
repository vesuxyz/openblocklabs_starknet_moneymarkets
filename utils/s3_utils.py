import logging
import awswrangler as wr
import pandas as pd

from datetime import datetime


def read_parquet_from_s3(bucket: str, s3_key: str) -> pd.DataFrame:
    """
    Reads a DataFrame from an Amazon S3 bucket in Parquet format.

    This function reads a Parquet file from a specified key within an Amazon S3 bucket
    and returns it as a pandas DataFrame.

    :param bucket: The name of the S3 bucket where the DataFrame will be read from.
    :type bucket: str
    :param s3_key: The S3 key that specifies the object name and format in the bucket.
    :type s3_key: str
    :return df: The parquet file returned as a pandas DataFrame
    :type df: pd.DataFrame
    """

    try:
        df = wr.s3.read_parquet(path=f"s3://{bucket}/{s3_key}")
        logging.info(
            f"Successfully loaded {df.shape[0]} records from s3://{bucket}/{s3_key}."
        )
    except Exception as e:
        raise Exception(
            f"An error occurred while uploading DataFrame to s3://{bucket}/{s3_key}: {e}"
        )
    return df


def load_parquet_to_s3(bucket: str, s3_key: str, df: pd.DataFrame) -> None:
    """
    Uploads a DataFrame to an Amazon S3 bucket in Parquet format.

    This function takes a pandas DataFrame and uploads it as a Parquet file
    to a specified key within an Amazon S3 bucket.

    :param bucket: The name of the S3 bucket where the DataFrame will be uploaded.
    :type bucket: str
    :param s3_key: The S3 key that specifies the object name and format in the bucket.
    :type s3_key: str
    :param df: The pandas DataFrame to upload.
    :type df: pd.DataFrame
    :return: None
    """
    if not df.empty:
        logging.info(f"Recieved new data")
        try:
            wr.s3.to_parquet(df=df, path=f"s3://{bucket}/{s3_key}", index=False)
            logging.info(
                f"Successfully loaded {df.shape[0]} new records to s3://{bucket}/{s3_key} as Parquet."
            )
        except Exception as e:
            raise Exception(
                f"An error occurred while uploading DataFrame to s3://{bucket}/{s3_key}: {e}"
            )
    else:
        logging.info(f"No new data to load")


def create_date_partition_key(timestamp: datetime):
    """
    Creates a partition key based on the provided timestamp.

    This function takes a datetime object as input and returns a partition 
    key in the format 'YYYY/MM/DD'.

    :param timestamp: The timestamp used to generate the partition key.
    :type timestamp: datetime.datetime
    :return: The partition key formatted as 'YYYY/MM/DD'.
    :rtype: str

    Example usage:
    >>> import datetime
    >>> timestamp = datetime.datetime(2023, 5, 1)
    >>> create_date_partition_key(timestamp)
    '2023/05/01'
    """
    return f"{timestamp.year}/{str(timestamp.month).zfill(2)}/{str(timestamp.day).zfill(2)}"