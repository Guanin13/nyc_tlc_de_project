from src.extract import build_url
from unittest.mock import Mock, patch
from src.extract import extract


def test_build_url():
    # Call build_url with a known Airflow execution date
    # The function should go 3 months back from 2025-04-01 -> 2025-01
    url, year, month = build_url("2025-04-01")

    # Check that the returned year is correct
    assert year == "2025"

    # Check that the returned month is correctly zero-padded
    assert month == "01"

    # Check that the final download URL is built correctly
    assert url == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"


# Patch external dependencies used inside extract()
@patch("src.extract.requests.get")
@patch("src.extract.boto3.client")
@patch("src.extract.os.getenv")
@patch("src.extract.build_url")
def test_extract_success(mock_build_url, mock_getenv, mock_boto_client, mock_requests_get):
    # 1. Mock the return value of build_url()
    mock_build_url.return_value = (
        "https://example.com/yellow_tripdata_2025-01.parquet",
        "2025",
        "01"
    )

    # 2. Mock environment variables
    def fake_getenv(key):
        values = {
            "AWS_ACCESS_KEY_ID": "fake_access_key",
            "AWS_SECRET_ACCESS_KEY": "fake_secret_key",
            "AWS_DEFAULT_REGION": "ap-southeast-2",
            "S3_BUCKET": "test-bucket"
        }
        return values.get(key)

    mock_getenv.side_effect = fake_getenv

    # 3. Mock the boto3 S3 client
    mock_s3 = Mock()
    mock_boto_client.return_value = mock_s3

    # 4. Mock the HTTP response object returned by requests.get()
    mock_response = Mock()
    mock_response.__enter__ = Mock(return_value=mock_response)
    mock_response.__exit__ = Mock(return_value=None)

    mock_response.raise_for_status = Mock()

    mock_response.raw = Mock()

    # Make requests.get() return the fake response
    mock_requests_get.return_value = mock_response

    # 5. Run the extract function
    # This should use all the mocked objects above
    extract("2025-04-01")

    # 6. Assertions

    # Check that build_url() was called with the expected date
    mock_build_url.assert_called_once_with("2025-04-01")

    # Check that boto3.client() was called correctly with mocked credentials
    mock_boto_client.assert_called_once_with(
        "s3",
        aws_access_key_id="fake_access_key",
        aws_secret_access_key="fake_secret_key",
        region_name="ap-southeast-2"
    )

    # Check that requests.get() was called with the expected URL and options
    mock_requests_get.assert_called_once_with(
        "https://example.com/yellow_tripdata_2025-01.parquet",
        stream=True,
        timeout=10
    )

    # Check that the HTTP status check was performed
    mock_response.raise_for_status.assert_called_once()

    # Check that the file stream was uploaded to the correct S3 bucket and key
    mock_s3.upload_fileobj.assert_called_once_with(
        mock_response.raw,
        "test-bucket",
        "raw/year=2025/month=01/yellow_tripdata_2025-01.parquet"
    )