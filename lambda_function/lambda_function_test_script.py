"""
Script to be used for testing the lambda handler locally
"""
from lambda_function import handler

if __name__ == "__main__":
    # Set Event to Lambda Handler With
    event = {}
    context = {}
    print(handler(event, context))
