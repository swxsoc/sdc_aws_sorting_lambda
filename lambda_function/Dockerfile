# Repo where this image's Dockerfile is maintained: https://github.com/HERMES-SOC/docker-lambda-base
ARG BASE_IMAGE=public.ecr.aws/w5r9l1c8/dev-swsoc-docker-lambda-base:latest

FROM ${BASE_IMAGE}

# Working Directory Arguments
ARG ROOT="/"
ARG FUNCTION_DIR="/lambda_function/"

COPY requirements.txt ${ROOT}

# Update pip and install setuptools
RUN pip install --upgrade pip setuptools

# Install requirements
RUN pip install -r requirements.txt

# Copy files from the source folder
COPY src/. ${FUNCTION_DIR}

# Set permissions for all files in function directory
RUN chmod -R 755 ${FUNCTION_DIR}
RUN chown -R 1000:1000 ${FUNCTION_DIR}

# Change working directory to /function
WORKDIR ${FUNCTION_DIR}

# Start the Lambda runtime interface client. AWS provides the Runtime API.
ENTRYPOINT [ "python3", "-m", "awslambdaric" ]

# Runs lambda handler function
CMD [ "lambda.handler" ]
