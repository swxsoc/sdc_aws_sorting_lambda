"""
This module contains the handler function and the main function which contains the logic
that initializes the FileSorter class in it's correct environment.

TODO: Skeleton Code for initial repo, logic still needs to be implemented
and docstrings expanded
"""

import json
import os


# The below flake exceptions are to avoid the hermes.log writing
# issue the above line solves
from hermes_core import log  # noqa: E402
from file_sorter.file_sorter import FileSorter  # noqa: E402

# This is so the hermes.log file writes to the correct location


def handler(event, context):
    """
    This is the lambda handler function that passes variables to the function that
    handles the logic that initializes the FileProcessor class in it's correct
    environment.
    """
    # Extract needed information from event
    try:
        for s3_event in event["Records"]:

            s3_bucket = s3_event["s3"]["bucket"]
            s3_object = s3_event["s3"]["object"]

            environment = os.getenv("LAMBDA_ENVIRONMENT")
            if environment is None:
                environment = "DEVELOPMENT"
            # Pass required variables to sort function and returns a 200 (Successful)
            # / 500 (Error) HTTP response
            response = sort_file(environment, s3_bucket, s3_object)

            return response

    except KeyError:

        return {
            "statusCode": 500,
            "body": json.dumps("Key Error Extracting Variables from Event"),
        }


def sort_file(environment, s3_bucket, s3_object):
    """
    This is the main function that handles logic that initializes the
    FileProcessor class in it's correct environment.
    """

    # Production (Official Release) Environment / Local Development
    try:
        log.info(f"Initializing FileSorter - Environment: {environment}")

        if environment == "PRODUCTION":
            FileSorter(s3_bucket=s3_bucket, s3_object=s3_object)
        else:
            FileSorter(s3_bucket=s3_bucket, s3_object=s3_object, dry_run=True)

        log.info("File Sorted Successfully")

        return {"statusCode": 200, "body": json.dumps("File Sorted Successfully")}

    except BaseException as e:
        log.error({"status": "ERROR", "message": e})

        return {"statusCode": 500, "body": json.dumps("Error Sorting File")}
