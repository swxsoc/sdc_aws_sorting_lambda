from aws_cdk import Stack, aws_lambda, aws_s3_notifications, aws_s3
from constructs import Construct
import logging


class SDCAWSSortingLambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Name of Incoming Bucket that will trigger the lambda
        bucket_name = "swsoc-incoming"

        # Get the incoming bucket from S3
        incoming_bucket = aws_s3.Bucket.from_bucket_name(
            self, "aws_sdc_incoming_bucket", bucket_name
        )

        # Create Sorting Lambda Function from Zip
        sdc_aws_sorting_function = aws_lambda.Function(
            scope=self,
            id="aws_sdc_sorting_function",
            handler="lambda_function.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            memory_size=128,
            function_name="aws_sdc_sorting_lambda_function",
            description=(
                "SWSOC Processing Lambda function deployed using AWS CDK Python"
            ),
            code=aws_lambda.AssetCode("lambda_function/"),
        )

        # Add Trigger to the Bucket to call Lambda
        incoming_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            aws_s3_notifications.LambdaDestination(sdc_aws_sorting_function),
        )

        logging.info("Function created successfully: %s", sdc_aws_sorting_function)
