========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - build status
      - |testing| |codestyle| |coverage|

.. |testing| image:: https://github.com/swxsoc/sdc_aws_sorting_lambda/actions/workflows/testing.yml/badge.svg
    :target: https://github.com/swxsoc/sdc_aws_sorting_lambda/actions/workflows/testing.yml
    :alt: testing status

.. |codestyle| image:: https://github.com/swxsoc/sdc_aws_sorting_lambda/actions/workflows/codestyle.yml/badge.svg
    :target: https://github.com/swxsoc/sdc_aws_sorting_lambda/actions/workflows/codestyle.yml
    :alt: codestyle and linting

.. |coverage| image:: https://codecov.io/gh/swxsoc/sdc_aws_sorting_lambda/graph/badge.svg?token=KHJfohC6yd
    :target: https://codecov.io/gh/swxsoc/sdc_aws_sorting_lambda
    :alt: code coverage

.. end-badges

This repository is to define the code to be used for the SWSOC file sorting Lambda function. 
This function will be deployed as a zip file to Lambda, with the production lambda being the latest release and the latest code on the master being used for development and testing. 
The production lambda will move files into the appropriate buckets while the development lambda will only move files with the prefix `dev_`. 

Running Unit Tests
------------------

.. code-block:: sh

    pytest --pyargs lambda_function/tests --cov=lambda_function/src --cov-report=html

Testing Lambda Locally
----------------------

To test the Lambda function locally using Docker:

1. Build the Lambda container image (from within the ``lambda_function`` folder):

    .. code-block:: sh

         docker build -t sdc_aws_sorting_lambda:latest .

2. Download the AWS Lambda Runtime Interface Emulator (RIE) outside the
   container image. RIE is only needed for local testing and is not included in
   the image deployed to AWS:

    .. code-block:: sh

         mkdir -p ~/.aws-lambda-rie
         curl -Lo ~/.aws-lambda-rie/aws-lambda-rie \
           https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/download/v1.36/aws-lambda-rie
         echo "ba57f2683260127135ad5ba9bafea141f90492143cbaeb9312cde6dae8d1c08e  $HOME/.aws-lambda-rie/aws-lambda-rie" \
           | shasum -a 256 -c -
         chmod +x ~/.aws-lambda-rie/aws-lambda-rie

3. Run the Lambda container image (after using your MFA script). Mounting RIE
   starts a local version of the Lambda Runtime API:

    .. code-block:: sh

         docker run \
           -p 9000:8080 \
           -v ~/.aws-lambda-rie:/aws-lambda \
           -v "$(pwd)/tests/test_data:/test_data" \
           --entrypoint /aws-lambda/aws-lambda-rie \
           sdc_aws_sorting_lambda:latest \
           python3 -m awslambdaric lambda.handler

4. From a separate terminal, make a curl request to the running Lambda function:

    .. code-block:: sh

         curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d @tests/test_data/test_padre_event.json


Acknowledgements
----------------
The package template used by this package is based on the one developed by the
`NASA Space Weather Science Operations Center (SWxSOC) <https://swxsoc.github.io>`_ which is based on those provided by
`OpenAstronomy community <https://openastronomy.org>`_ and the `SunPy Project <https://sunpy.org/>`_.

This project makes use of the `NASA Space Weather Science Operations Center (SWxSOC) <https://swxsoc.github.io>`_.
