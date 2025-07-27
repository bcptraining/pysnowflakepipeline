import pytest


@pytest.fixture
def mock_df():
    return lambda cols: type("MockDF", (), {"columns": cols})()
