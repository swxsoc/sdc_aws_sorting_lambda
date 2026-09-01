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

This repository defines the SWxSOC file-sorting Lambda container. The function
moves incoming files to mission and instrument buckets. In ``DEVELOPMENT`` it
only moves files with the ``dev_`` prefix; in ``PRODUCTION`` it processes all
matching files.

Runtime Environment Variables
-----------------------------

- ``LAMBDA_ENVIRONMENT``: ``DEVELOPMENT`` or ``PRODUCTION`` sorting behavior.
- ``SWXSOC_MISSION``: Mission configuration identifier such as ``hermes``,
  ``padre``, ``impax``, or ``swxsoc_pipeline``.
- Communication configuration and credentials are supplied by the architecture
  Terraform for missions that enable notifications.

Automated Deployment
--------------------

AWS CodeBuild derives the mission from its project name and publishes the
container to that mission's ECR repository. A build of ``main`` targets the
development repository by default; a release tag targets production.
``CDK_ENVIRONMENT`` or ``ENVIRONMENT`` may explicitly select ``dev``/
``development`` or ``prod``/``production``. Conflicting or unknown values are
rejected. Pull requests and other branches do not push or deploy images.

Mission base-image builds can pass ``PUBLIC_ECR_REPO``. Sorting uses that exact
versioned base-image reference after validating its namespace, repository,
environment, and non-``latest`` tag. If no pinned reference is provided, a
mission build uses its environment's ``latest`` base image.
After publishing the Lambda image, CodeBuild sends its immutable image tag to
the mission architecture project for Terraform deployment.

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
