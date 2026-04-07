from datetime import datetime
from dateutil.relativedelta import relativedelta
import boto3
import os
import requests
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()


def build_url(date: str):
    """
    Build the NYC taxi parquet file URL for the month
    that is 3 months before the given execution date.

    Args:
        date (str): Execution date in 'YYYY-MM-DD' format.

    Returns:
        tuple: (url, year, month)
            url   -> download link for the parquet file
            year  -> target year as string
            month -> target month as zero-padded string
    """

    # Convert the input string into a datetime object
    date_3_months_ago = datetime.strptime(date, "%Y-%m-%d") - relativedelta(months=3)

    # Extract year and month from the adjusted date
    year = str(date_3_months_ago.year)
    month = f"{date_3_months_ago.month:02d}"

    # Build the download URL for the parquet file
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
    
    return url, year, month


def extract(ds: str, **kwargs) -> None:
    """
    Download the NYC yellow taxi parquet file for the month
    3 months before the Airflow execution date, then upload it to S3.

    Args:
        ds (str): Airflow execution date in 'YYYY-MM-DD' format.
        **kwargs: Extra Airflow context arguments (not used here).

    Returns:
        None
    """

    # Generate the source file URL and the target year/month folder values
    url, year, month = build_url(ds)

    # Create an S3 client using AWS credentials stored in environment variables
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )

    # Get the target S3 bucket name from environment variables
    bucket = os.getenv('S3_BUCKET')

    # Download the parquet file as a stream instead of loading it fully into memory
    with requests.get(url, stream=True, timeout=10) as r:
        # Raise an error if the HTTP request failed
        r.raise_for_status()

        # Upload the streamed file directly to S3
        # S3 key structure uses partition-style folders: raw/year=.../month=...
        s3.upload_fileobj(
            r.raw,
            bucket,
            f'raw/year={year}/month={month}/yellow_tripdata_{year}-{month}.parquet'
        )