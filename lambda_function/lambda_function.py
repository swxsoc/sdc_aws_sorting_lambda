"""
This module contains the handler function and the main function which contains the logic
that initializes the FileSorter class in it's correct environment.

TODO: Skeleton Code for initial repo, logic still needs to be implemented
and docstrings expanded
"""

import logging
import json
import os
from file_sorter.file_sorter import FileSorter

logger = logging.getLogger()
# Debug used for development
logger.setLevel(logging.DEBUG)


def handler(event, context):
    """
    This is the lambda handler function that passes variables to the function that
    handles the logic that initializes the FileProcessor class in it's correct
    environment.
    """
    # Extract needed information from event
    try:
        for s3_event in event['Records']:
                
            s3_bucket = s3_event["s3"]["bucket"]
            s3_object = s3_event["s3"]["object"]

            print(s3_bucket)
            print(s3_object)

            # environment = os.getenv("LAMBDA_ENVIRONMENT")
            environment = "PRODUCTION"
            # Pass required variables to sort function and returns a 200 (Successful)
            # / 500 (Error) HTTP response
            response = sort_file(environment, s3_bucket, s3_object)

            return response

    except KeyError:
        
        return {
            "statusCode": 500,
            "body": json.dumps(f"Key Error Extracting Variables from Event"),
        }


def sort_file(environment, s3_bucket, s3_object):
    """
    This is the main function that handles logic that initializes the
    FileProcessor class in it's correct environment.
    """

    # Production (Official Release) Environment / Local Development
    if environment == "PRODUCTION":
        try:
            logger.info("Initializing FileSorter - Environment: Production")
            sorter = FileSorter(s3_bucket=s3_bucket, s3_object=s3_object)
            logger.warning("FileSorter Initialized Successfully")

            return {"statusCode": 200, "body": json.dumps("File Sorted Successfully")}

        except BaseException as e:
            logger.error("Error occurred with FileSorter: %s", e)

            return {"statusCode": 500, "body": json.dumps("Error Sorting File")}

    # Development (Master Branch but not Official Release) Environment
    elif environment == "DEVELOPMENT":
        try:
            logger.info("Initializing FileSorter - Environment: Development")
            sorter = FileSorter(s3_bucket=s3_bucket, s3_object=s3_object, dry_run=True)
            logger.warning("FileSorter Initialized Successfully")
            sorter.sort_file()

            return {"statusCode": 200, "body": json.dumps("File Sorted Successfully")}

        except BaseException as e:
            logger.error("Error occurred with FileSorter: %s", e)

            return {"statusCode": 500, "body": json.dumps("Error Sorting File")}

    else:
        return {"statusCode": 500, "body": json.dumps("Invalid key for environment")}
