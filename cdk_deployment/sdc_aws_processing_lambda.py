import os
from aws_cdk import Stack, aws_lambda, aws_ecr
from constructs import Construct
import logging


class SDCAWSSortingLambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Container Image ECR Function
        sdc_aws_sorting_function = aws_lambda.Function(
            scope=self,
            id=f"{repo_name}_function",
            function_name=f"{repo_name}_function",
            description=(
                "SWSOC Processing Lambda function deployed using AWS CDK Python"
            ),
            code=aws_lambda.DockerImageCode.AssetCode("lambda_function/"),
        )

        logging.info("Function created successfully: %s", sdc_aws_sorting_function)
