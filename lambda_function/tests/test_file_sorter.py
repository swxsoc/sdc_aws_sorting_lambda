"""
This module tests the FileSorter class and it's functions
TODO: This is skeleton test for starter repo, still needs to be implemented
"""

from file_sorter.file_sorter import FileSorter


def test_initializing_class():
    """
    Test function that tests if class initializes correctly if passed correct variables.
    """

    test_s3_bucket = {"name": "test_bucket"}
    test_file_key = {
        "key": "hermes_spn_2s_l3test_burst_20240406_120621_v2.4",
        "eTag": "12345678",
    }

    test_sorter = FileSorter(
        test_s3_bucket, test_file_key, environment="PRODUCTION", dry_run=True
    )

    assert test_sorter is not None
