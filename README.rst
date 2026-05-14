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

2. Run the Lambda container image (after using your MFA script). This starts the Lambda runtime environment:

    .. code-block:: sh

         docker run \
           -p 9000:8080 \
           -v "$(pwd)/tests/test_data:/test_data" \
           sdc_aws_sorting_lambda:latest

3. From a separate terminal, make a curl request to the running Lambda function:

    .. code-block:: sh

         curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d @tests/test_data/test_padre_event.json


Acknowledgements
----------------
The package template used by this package is based on the one developed by the
`NASA Space Weather Science Operations Center (SWxSOC) <https://swxsoc.github.io>`_ which is based on those provided by
`OpenAstronomy community <https://openastronomy.org>`_ and the `SunPy Project <https://sunpy.org/>`_.

This project makes use of the `NASA Space Weather Science Operations Center (SWxSOC) <https://swxsoc.github.io>`_.
