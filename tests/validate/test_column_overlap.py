import sys
import os
from snow_pipeline_pkg.validate.quality_checks import validate_column_overlap

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# from validate.quality_checks import validate_column_overlap
from snow_pipeline_pkg.validate.quality_checks import validate_column_overlap


#  This was replaced by conftest.py
# class MockDataFrame:
#     def __init__(self, columns):
#         self.columns = columns


def test_overlap_detected(mock_df):
    df = mock_df(["id", "email", "last_name"])
    expected = ["USER_ID", "EMAIL", "FIRST_NAME"]
    assert validate_column_overlap(df, expected) is True


def test_no_overlap_detected(mock_df):
    df = mock_df(["foo", "bar", "baz"])
    expected = ["FIRST_NAME", "EMAIL"]
    assert validate_column_overlap(df, expected) is False


def test_case_insensitivity(mock_df):
    df = mock_df(["Id", "EMAIL"])
    expected = ["id", "email"]
    assert validate_column_overlap(df, expected) is True
