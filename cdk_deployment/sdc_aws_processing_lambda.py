from aws_cdk import Stack, aws_lambda, aws_lambda_event_sources, aws_s3
from constructs import Construct
import logging


class SDCAWSSortingLambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Name of Incoming Bucket that will trigger the lambda
        bucket_name = "swsoc-incoming"

        # Get the incoming bucket from S3
        incoming_bucket = aws_s3.Bucket.from_bucket_name(scope, id, bucket_name)

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

        # Add Object Created trigger to the Lambda
        sdc_aws_sorting_function.add_event_source(
            aws_lambda_event_sources.S3EventSource(
                incoming_bucket,
                events=[
                    aws_s3.EventType.OBJECT_CREATED,
                    aws_s3.EventType.OBJECT_REMOVED,
                ],
            )
        )

        logging.info("Function created successfully: %s", sdc_aws_sorting_function)
