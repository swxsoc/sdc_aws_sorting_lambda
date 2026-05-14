import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws as moto_mock_aws
from sdc_aws_utils.aws import create_s3_file_key
from sdc_aws_utils.config import get_incoming_bucket, get_instrument_bucket, parser
from src.file_sorter import file_sorter

TEST_REGION = "us-east-1"
ENVIRONMENT = "PRODUCTION"


POSITIVE_CASES = [
    # HERMES
    {
        "mission": "hermes",
        "instrument": "eea",
        "file_key": "/tests/test_files/hermes_EEA_l0_2025337-124603_v11.bin",
    },
    {
        "mission": "hermes",
        "instrument": "nemisis",
        "file_key": "/tests/test_files/hermes_NEM_l0_2024094-124603_v01.bin",
    },
    {
        "mission": "hermes",
        "instrument": "merit",
        "file_key": "/tests/test_files/hermes_MERIT_l0_2025215-124603_v21.bin",
    },
    {
        "mission": "hermes",
        "instrument": "spani",
        "file_key": "/tests/test_files/hermes_spn_2s_l3test_burst_20240406T120621_v2.4.5.cdf",
    },
    # PADRE
    {
        "mission": "padre",
        "instrument": "meddea",
        "file_key": "/tests/test_files/padre_MEDDEA_l0_2025131-192102_v3.bin",
    },
    {
        "mission": "padre",
        "instrument": "sharp",
        "file_key": "/tests/test_files/padre_sharp_ql_20230430T000000_v0.0.1.fits",
    },
    {
        "mission": "padre",
        "instrument": "craft",
        "file_key": "/tests/test_files/padre_get_EPS_9_Data_1762008094193_1762187403300.csv",
    },
    # SWxSOC Pipeline
    {
        "mission": "swxsoc_pipeline",
        "instrument": "reach",
        "file_key": "/tests/test_files/REACH-ALL_20251205T060517_20251205T060517.csv",
    },
    {
        "mission": "swxsoc_pipeline",
        "instrument": "reach",
        "file_key": "/tests/test_files/reach_all_l1c_prelim_20260101T000000_v1.0.1.cdf",
    },
]


NEGATIVE_INVALID_FILE_KEY = "/tests/test_files/test-file-key.txt"
NEGATIVE_MISSING_FILE_KEY = "/tests/test_files/missing-file-key.txt"


# Fixtures
@pytest.fixture(scope="function")
def aws_credentials():
    """AWS Credentials fixture"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def mock_aws():
    """Mock AWS services using moto."""
    with moto_mock_aws():
        yield


@pytest.fixture(scope="function")
def s3_client(aws_credentials, mock_aws):
    """S3 client fixture"""
    conn = boto3.client("s3", region_name=TEST_REGION)
    yield conn


@pytest.fixture(scope="function")
def timestream_client(aws_credentials, mock_aws):
    """Timestream client fixture"""
    conn = boto3.client("timestream-write", region_name=TEST_REGION)
    yield conn


# Utility Functions
def create_s3_event(bucket_name, object_key):
    """
    Create a mock S3 event payload.

    Parameters
    ----------
    bucket_name : str
        The name of the S3 bucket that received the object.
    object_key : str
        The key (path) of the object within the bucket.

    Returns
    -------
    dict
        A dictionary representing an S3 event with a single ``ObjectCreated:Put``
        record, matching the structure delivered by AWS Lambda triggers.
    """
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2023-08-12T12:34:56.789Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {"principalId": "EXAMPLE"},
                "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE",
                    "x-amz-id-2": "EXAMPLE",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": bucket_name,
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": "arn:aws:s3:::{}".format(bucket_name),
                    },
                    "object": {
                        "key": object_key,
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901",
                    },
                },
            }
        ]
    }


def setup_environment(
    s3_client,
    timestream_client,
    incoming_bucket,
    destination_buckets,
    existing_object_keys,
):
    """
    Set up AWS resources and environment variables required for testing.

    Creates the necessary S3 buckets and uploads test objects, creates the
    Timestream database and table, and sets Lambda-related environment
    variables.  Silently ignores ``ConflictException`` when the Timestream
    resources already exist.

    Parameters
    ----------
    s3_client : botocore.client.S3
        A mocked boto3 S3 client (provided by the ``s3_client`` fixture).
    timestream_client : botocore.client.TimestreamWrite
        A mocked boto3 Timestream Write client (provided by the
        ``timestream_client`` fixture).

    Returns
    -------
    None
    """

    def _get_timestream_names(environment):
        """Return mission-aware Timestream database and table names."""
        mission_name = os.getenv("SWXSOC_MISSION")
        if not mission_name or mission_name == "hermes":
            database_name = "sdc_aws_logs"
            table_name = "sdc_aws_s3_bucket_log_table"
        else:
            database_name = f"{mission_name}_sdc_aws_logs"
            table_name = f"{mission_name}_sdc_aws_s3_bucket_log_table"

        if environment == "DEVELOPMENT":
            database_name = f"dev-{database_name}"
            table_name = f"dev-{table_name}"

        return database_name, table_name

    # Create buckets in S3
    s3_client.create_bucket(Bucket=incoming_bucket)
    for bucket in destination_buckets:
        s3_client.create_bucket(Bucket=bucket)

    for key in existing_object_keys:
        s3_client.put_object(Bucket=incoming_bucket, Key=key, Body=b"test file")

    # Set up the database and table in Timestream
    database_name, table_name = _get_timestream_names(ENVIRONMENT)
    try:
        timestream_client.create_database(DatabaseName=database_name)
    except timestream_client.exceptions.ConflictException:
        pass
    try:
        timestream_client.create_table(DatabaseName=database_name, TableName=table_name)
    except timestream_client.exceptions.ConflictException:
        pass

    os.environ["LAMBDA_ENVIRONMENT"] = ENVIRONMENT
    os.environ["SDC_AWS_SLACK_TOKEN"] = "test-token"
    os.environ["SDC_AWS_SLACK_CHANNEL"] = "test-channel"


def setup_case_environment(s3_client, timestream_client, instrument, file_key):
    """Create case-specific resources for an incoming file and target instrument bucket."""
    incoming_bucket = get_incoming_bucket(ENVIRONMENT)
    destination_bucket = get_instrument_bucket(instrument, ENVIRONMENT)
    setup_environment(
        s3_client=s3_client,
        timestream_client=timestream_client,
        incoming_bucket=incoming_bucket,
        destination_buckets=[destination_bucket],
        existing_object_keys=[file_key],
    )
    return incoming_bucket, destination_bucket


def assert_file_sorted(s3_client, destination_bucket, file_key):
    """Assert that a file was written to the expected destination key."""
    expected_key = create_s3_file_key(parser, Path(file_key).name)
    objects = s3_client.list_objects(Bucket=destination_bucket).get("Contents")
    assert objects
    assert objects[0].get("Key") == expected_key


# Tests handle_event Function
@pytest.mark.parametrize(
    ("use_mission", "case"),
    [
        pytest.param(
            case["mission"],
            case,
            id=f"{case['mission']}-{case['instrument']}-{Path(case['file_key']).suffix.lstrip('.')}",
        )
        for case in POSITIVE_CASES
    ],
    indirect=["use_mission"],
)
def test_file_sorter(s3_client, timestream_client, use_mission, case):
    """Test successful sorting across supported missions and instruments."""
    incoming_bucket, destination_bucket = setup_case_environment(
        s3_client=s3_client,
        timestream_client=timestream_client,
        instrument=case["instrument"],
        file_key=case["file_key"],
    )

    s3_event = create_s3_event(incoming_bucket, case["file_key"])
    response = file_sorter.handle_event(event=s3_event, context=None)

    # Successful run should return 200 status code
    assert response["statusCode"] == 200
    assert_file_sorted(s3_client, destination_bucket, case["file_key"])


def test_file_sorter_missing_file(s3_client, timestream_client):
    """Test handling of an S3 event where the specified file is missing from the bucket."""
    incoming_bucket, destination_bucket = setup_case_environment(
        s3_client=s3_client,
        timestream_client=timestream_client,
        instrument="spani",
        file_key=NEGATIVE_INVALID_FILE_KEY,
    )

    s3_event = create_s3_event(incoming_bucket, NEGATIVE_MISSING_FILE_KEY)
    response = file_sorter.handle_event(event=s3_event, context=None)

    assert response["statusCode"] == 500
    assert not s3_client.list_objects(Bucket=destination_bucket).get("Contents")


def test_file_sorter_bad_file(s3_client, timestream_client):
    """Test handling of an S3 event where the specified file is invalid or cannot be processed."""
    incoming_bucket, destination_bucket = setup_case_environment(
        s3_client=s3_client,
        timestream_client=timestream_client,
        instrument="spani",
        file_key=NEGATIVE_INVALID_FILE_KEY,
    )

    s3_event = create_s3_event(incoming_bucket, NEGATIVE_INVALID_FILE_KEY)
    response = file_sorter.handle_event(event=s3_event, context=None)

    assert response["statusCode"] == 500
    assert not s3_client.list_objects(Bucket=destination_bucket).get("Contents")


def test_file_sorter_missing_s3_bucket(s3_client):
    """Test handling of an S3 event where the specified bucket is missing."""

    s3_event = create_s3_event("missing-incoming-bucket", NEGATIVE_INVALID_FILE_KEY)
    response = file_sorter.handle_event(event=s3_event, context=None)

    assert response["statusCode"] == 500


@pytest.mark.parametrize(
    "use_mission", ["hermes", "padre", "swxsoc_pipeline"], indirect=True
)
def test_file_sorter_empty_trigger(s3_client, timestream_client, use_mission):
    """Test handling of an empty trigger event."""
    instrument = (
        "spani"
        if use_mission == "hermes"
        else "meddea"
        if use_mission == "padre"
        else "reach"
    )
    file_key = (
        "/tests/test_files/hermes_SPANI_l0_2023040-000018_v01.bin"
        if use_mission == "hermes"
        else "/tests/test_files/padre_MEDDEA_l0_2025131-192102_v3.bin"
        if use_mission == "padre"
        else "/tests/test_files/REACH-ALL_20251205T060517_20251205T060517.csv"
    )

    _incoming_bucket, destination_bucket = setup_case_environment(
        s3_client=s3_client,
        timestream_client=timestream_client,
        instrument=instrument,
        file_key=file_key,
    )

    trigger_event = {}
    response = file_sorter.handle_event(event=trigger_event, context=None)

    assert response["statusCode"] == 200

    # Ensure no crash when file already exists in target bucket.
    s3_client.put_object(
        Bucket=destination_bucket,
        Key=create_s3_file_key(parser, Path(file_key).name),
        Body=b"test file",
    )
    response = file_sorter.handle_event(event=trigger_event, context=None)

    assert response["statusCode"] == 200


# Tests FileSorter class
@pytest.mark.parametrize(
    ("use_mission", "instrument", "file_key"),
    [
        pytest.param(
            "hermes",
            "spani",
            "/tests/test_files/hermes_SPANI_l0_2023040-000018_v01.bin",
            id="hermes",
        ),
        pytest.param(
            "padre",
            "meddea",
            "/tests/test_files/padre_MEDDEA_l0_2025131-192102_v3.bin",
            id="padre",
        ),
        pytest.param(
            "swxsoc_pipeline",
            "reach",
            "/tests/test_files/reach_all_l1c_prelim_20260101T000000_v1.0.1.cdf",
            id="swxsoc_pipeline",
        ),
    ],
    indirect=["use_mission"],
)
def test_file_sorter_dry_run(
    s3_client, timestream_client, use_mission, instrument, file_key
):
    """Test Dry-Run Mode doesn't move files in S3"""
    incoming_bucket, destination_bucket = setup_case_environment(
        s3_client=s3_client,
        timestream_client=timestream_client,
        instrument=instrument,
        file_key=file_key,
    )

    file_sorter.FileSorter(
        s3_bucket=incoming_bucket,
        file_key=file_key,
        environment=ENVIRONMENT,
        dry_run=True,
        s3_client=s3_client,
        timestream_client=timestream_client,
    )

    assert not s3_client.list_objects(Bucket=destination_bucket).get("Contents")


def test_file_sorter_missing_timestream(s3_client):
    """Test handling of missing Timestream client during FileSorter initialization."""
    test_incoming_bucket = get_incoming_bucket("DEVELOPMENT")
    test_target_bucket = get_instrument_bucket("spani", "DEVELOPMENT")
    test_file_key = "/tests/test_files/hermes_SPANI_l0_2023040-000018_v01.bin"
    s3_client.create_bucket(Bucket=test_incoming_bucket)
    s3_client.create_bucket(Bucket=test_target_bucket)
    s3_client.put_object(
        Bucket=test_incoming_bucket, Key=test_file_key, Body=b"test file"
    )
    try:
        file_sorter.FileSorter(
            test_incoming_bucket,
            test_file_key,
            "DEVELOPMENT",
            dry_run=False,
            s3_client=s3_client,
            timestream_client="Invalid",
        )
        # Should not reach here
        assert False
    except Exception as e:
        assert e is not None
