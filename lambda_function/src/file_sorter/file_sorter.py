"""
FileSorter class that will sort the files into the appropriate
HERMES instrument folder.
"""

import os
import json
from pathlib import Path

from slack_sdk.errors import SlackApiError
from swxsoc.util.util import parse_science_filename
from sdc_aws_utils.logging import log, configure_logger
from sdc_aws_utils.aws import (
    create_s3_client_session,
    create_timestream_client_session,
    copy_file_in_s3,
    log_to_timestream,
    object_exists,
    check_file_existence_in_target_buckets,
    create_s3_file_key,
    list_files_in_bucket,
)
from sdc_aws_utils.slack import get_slack_client, send_pipeline_notification
from sdc_aws_utils.config import (
    get_incoming_bucket,
    get_instrument_bucket,
    get_all_instrument_buckets,
)

# Configure logging levels and format
configure_logger()


def handle_event(event, context):
    """
    Initialize the FileSorter class in the appropriate environment.

    :param event: The triggering AWS Lambda event
    :param context: The AWS Lambda context object
    :return: The response object with status code and message
    :rtype: dict
    """

    environment = os.getenv("LAMBDA_ENVIRONMENT", "DEVELOPMENT")
    if "Records" in event:
        try:
            for s3_event in event["Records"]:
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
        dry_run=False,
        s3_client: type = None,
        timestream_client: type = None,
    ):
        """
        Initialize the FileSorter object.
        """
        try:
            # Initialize the slack client
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

        try:
            self.timestream_client = (
                timestream_client or create_timestream_client_session()
            )
        except Exception as e:
            log.error(f"Error creating Timestream client: {e}")
            self.timestream_client = None

        self.s3_client = s3_client or create_s3_client_session()
        try:
            self.science_file = parse_science_filename(self.file_key)
        except Exception as e:
            log.error(f"Issue parsing file: {self.file_key}")
            raise e
        self.incoming_bucket_name = s3_bucket
        self.destination_bucket = get_instrument_bucket(
            self.science_file["instrument"], environment
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
            object_exists(
                s3_client=self.s3_client,
                bucket=self.incoming_bucket_name,
                file_key=self.file_key,
            )
            or self.dry_run
        ):
            try:
                # Get file name from file key
                path_file = Path(self.file_key)
                new_file_key = create_s3_file_key(
                    parse_science_filename, path_file.name
                )
            except ValueError:
                log.warning(f"Error parsing file key: {self.file_key}")
                return None

            log.info(
                f"Copying {self.file_key} from {self.incoming_bucket_name}"
                f"to {self.destination_bucket}"
            )

            if not self.dry_run:
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

        else:
            raise ValueError("File does not exist in bucket")
