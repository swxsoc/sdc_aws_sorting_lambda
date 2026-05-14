"""
Handler function and the main function for the AWS Lambda,
which initializes the FileSorter class in the appropriate environment.
"""

from typing import Any

from file_sorter import file_sorter


def handler(event: dict[str, Any], context: Any) -> dict[str, int | str]:
    """
    Handle the AWS Lambda invocation.

    Parameters
    ----------
    event : dict[str, Any]
        Lambda event payload, typically containing S3 event records.
    context : Any
        AWS Lambda runtime context object.

    Returns
    -------
    dict[str, int | str]
        Response dictionary containing ``statusCode`` and serialized ``body``.
    """

    return file_sorter.handle_event(event, context)
