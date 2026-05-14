"""
FileSorter class that will sort the files into the appropriate instrument folder.
"""

import json
import os
from pathlib import Path
from typing import Any

from botocore.client import BaseClient
from sdc_aws_utils.aws import (
    check_file_existence_in_target_buckets,
    copy_file_in_s3,
    create_s3_client_session,
    create_s3_file_key,
    create_timestream_client_session,
    list_files_in_bucket,
    log_to_timestream,
    object_exists,
)
from sdc_aws_utils.config import (
    get_all_instrument_buckets,
    get_incoming_bucket,
    get_instrument_bucket,
)
from sdc_aws_utils.logging import configure_logger, log
from sdc_aws_utils.slack import get_slack_client, send_pipeline_notification
from slack_sdk.errors import SlackApiError
from swxsoc.util.util import parse_science_filename

# Configure logging levels and format
configure_logger()


def handle_event(event: dict[str, Any], context: Any) -> dict[str, int | str]:
    """
    Process a Lambda event and dispatch file sorting work.

    Parameters
    ----------
    event : dict[str, Any]
        Triggering AWS Lambda event. Supports S3 ``Records`` events and empty
        events that trigger a full incoming-bucket scan and sorting of all files.
    context : Any
        AWS Lambda context object (accepted for compatibility).

    Returns
    -------
    dict[str, int | str]
        Response dictionary containing ``statusCode`` and serialized ``body``.
    """

    environment = os.getenv("LAMBDA_ENVIRONMENT", "DEVELOPMENT")
    if "Records" in event:
        try:
            for s3_event in event["Records"]:
                log.info(f"Processing S3 event: {s3_event}")
                s3_bucket = s3_event["s3"]["bucket"]["name"]
                file_key = s3_event["s3"]["object"]["key"]
                FileSorter(s3_bucket, file_key, environment)
            return {"statusCode": 200, "body": json.dumps("Success Sorting File")}

        except Exception as e:
            return {"statusCode": 500, "body": json.dumps(f"Error: {e}")}

    else:
        log.info("No records found in event. Checking all files in bucket.")
        s3_client = create_s3_client_session()
        incoming_bucket = get_incoming_bucket(environment)
        instrument_buckets = get_all_instrument_buckets(environment)
        keys_in_s3 = list_files_in_bucket(s3_client, incoming_bucket)
        for key in keys_in_s3:
            try:
                # Get file name from file key
                path_file = Path(key)
                parsed_file_key = create_s3_file_key(
                    parse_science_filename, path_file.name
                )
            except ValueError:
                continue

            if check_file_existence_in_target_buckets(
                s3_client, parsed_file_key, incoming_bucket, instrument_buckets
            ):
                continue

            log.info(f"File {parsed_file_key} does not exist in target buckets.")
            try:
                # Assign the s3_bucket variable here
                s3_bucket = incoming_bucket
                FileSorter(s3_bucket, key, environment)
            except Exception as e:
                log.error(f"Error sorting file {parsed_file_key}: {e}")
                continue
        log.info("Finished sorting all files in bucket.")
        return {"statusCode": 200, "body": json.dumps("Success Sorting Files")}


class FileSorter:
    """
    The FileSorter class initializes an object with the data file and the
    bucket event that triggered the lambda function call.
    """

    def __init__(
        self,
        s3_bucket: str,
        file_key: str,
        environment: str,
        dry_run: bool = False,
        s3_client: BaseClient | None = None,
        timestream_client: BaseClient | None = None,
    ) -> None:
        """
        Initialize sorter dependencies and process the requested file.

        Parameters
        ----------
        s3_bucket : str
            Source incoming S3 bucket name.
        file_key : str
            Source object key to sort.
        environment : str
            Deployment environment name.
        dry_run : bool, optional
            If ``True``, skip S3 copy/delete side effects.
        s3_client : BaseClient | None, optional
            Preconfigured S3 client. If ``None``, one is created.
        timestream_client : BaseClient | None, optional
            Preconfigured Timestream client. If ``None``, one is created.
        """
        log.info("Initializing FileSorter with parameters:")
        log.info(f"S3 Bucket: {s3_bucket}")
        log.info(f"File Key: {file_key}")
        log.info(f"Environment: {environment}")
        log.info(f"Dry Run: {dry_run}")

        try:
            # Initialize the slack client
            log.info("Initializing Slack client.")
            self.slack_client = get_slack_client(
                slack_token=os.getenv("SDC_AWS_SLACK_TOKEN")
            )

            # Initialize the slack channel
            self.slack_channel = os.getenv("SDC_AWS_SLACK_CHANNEL")
        except SlackApiError as e:
            error_code = int(e.response["Error"]["Code"])
            self.slack_client = None
            if error_code == 404:
                log.error(
                    {
                        "status": "ERROR",
                        "message": "Slack Token is invalid",
                    }
                )

        self.file_key = file_key

        # Send Initial Slack Notification about file upload
        if self.slack_client:
            log.info("Sending upload notification to Slack.")
            send_pipeline_notification(
                slack_client=self.slack_client,
                slack_channel=self.slack_channel,
                path=self.file_key,
                bucket_name=s3_bucket,
                alert_type="upload",
            )
        else:
            log.info("Slack client not initialized; skipping upload notification.")

        try:
            log.info("Initializing Timestream client.")
            self.timestream_client = (
                timestream_client or create_timestream_client_session()
            )
        except Exception as e:
            log.error(f"Error creating Timestream client: {e}")
            self.timestream_client = None

        log.info("Initializing S3 client.")
        self.s3_client = s3_client or create_s3_client_session()

        try:
            log.info(f"Parsing science filename: {self.file_key}")
            self.science_file = parse_science_filename(self.file_key)
        except Exception as e:
            log.error(f"Issue parsing file: {self.file_key}")
            raise e

        self.incoming_bucket_name = s3_bucket
        self.destination_bucket = get_instrument_bucket(
            self.science_file["instrument"], environment
        )
        log.info(
            f"Sorting from Incoming Bucket: {self.incoming_bucket_name} to Destination Bucket: {self.destination_bucket}"
        )

        self.dry_run = dry_run
        if self.dry_run:
            log.warning("Performing Dry Run - Files will not be copied/removed")

        self.environment = environment
        self._sort_file()

    def _sort_file(self):
        """
        Determine the correct sorting function based on the file key name.
        """

        if (
            not object_exists(
                s3_client=self.s3_client,
                bucket=self.incoming_bucket_name,
                file_key=self.file_key,
            )
            and not self.dry_run
        ):
            log.error(
                f"File {self.file_key} does not exist in bucket {self.incoming_bucket_name}"
            )
            raise ValueError("File does not exist in bucket")

        # Try to parse the file key and create the new file key for the destination bucket
        try:
            # Get file name from file key
            path_file = Path(self.file_key)
            new_file_key = create_s3_file_key(parse_science_filename, path_file.name)
        except ValueError:
            log.warning(f"Error parsing file key: {self.file_key}")
            return None

        log.info(
            f"Copying {self.file_key} from {self.incoming_bucket_name}"
            f"to {self.destination_bucket}"
        )

        if self.dry_run:
            log.info(
                f"Dry Run: Skipping copy of {self.file_key} to {self.destination_bucket}"
            )
            return None

        # Copy file from source to destination
        copy_file_in_s3(
            s3_client=self.s3_client,
            source_bucket=self.incoming_bucket_name,
            destination_bucket=self.destination_bucket,
            file_key=self.file_key,
            new_file_key=new_file_key,
        )

        # If Slack is enabled, send a slack notification
        if self.slack_client:
            send_pipeline_notification(
                slack_client=self.slack_client,
                slack_channel=self.slack_channel,
                path=new_file_key,
                bucket_name=self.incoming_bucket_name,
                alert_type="sorted",
            )

        # If Timestream is enabled, log the file
        if self.timestream_client:
            log_to_timestream(
                timestream_client=self.timestream_client,
                action_type="PUT",
                file_key=self.file_key,
                new_file_key=new_file_key,
                source_bucket=self.incoming_bucket_name,
                destination_bucket=self.destination_bucket,
                environment=self.environment,
            )

        log.info(
            f"File {self.file_key} Successfully Moved to {self.destination_bucket}"
        )
