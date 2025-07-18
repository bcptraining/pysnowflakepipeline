import pytest
from snow_pipeline_pkg.validate.quality_checks import validate_schema_matches_table


@pytest.fixture
def mock_df():
    def _make(columns):
        return type("MockDF", (), {"columns": columns})()

    return _make


@pytest.fixture
def mock_logger():
    class MockLogger:
        def __init__(self):
            self.messages = []

        def warning(self, msg):
            self.messages.append(f"WARNING: {msg}")

        def info(self, msg):
            self.messages.append(f"INFO: {msg}")

    return MockLogger()


def test_perfect_schema_match(mock_df, mock_logger):
    df = mock_df(["id", "name", "email"])
    expected = ["email", "name", "ID"]
    missing, extras = validate_schema_matches_table(df, expected, mock_logger)
    assert missing == []
    assert extras == []
    assert any("All schema columns matched" in msg for msg in mock_logger.messages)


def test_missing_columns(mock_df, mock_logger):
    df = mock_df(["id", "email"])
    expected = ["id", "email", "name"]
    missing, extras = validate_schema_matches_table(df, expected, mock_logger)
    assert missing == ["NAME"]
    assert extras == []
    assert any("Missing expected columns" in msg for msg in mock_logger.messages)


def test_extra_columns(mock_df, mock_logger):
    df = mock_df(["id", "email", "nickname"])
    expected = ["id", "email"]
    missing, extras = validate_schema_matches_table(df, expected, mock_logger)
    assert missing == []
    assert extras == ["NICKNAME"]
    assert any("Unused columns" in msg for msg in mock_logger.messages)


def test_missing_and_extra(mock_df, mock_logger):
    df = mock_df(["id", "nickname"])
    expected = ["id", "email"]
    missing, extras = validate_schema_matches_table(df, expected, mock_logger)
    assert missing == ["EMAIL"]
    assert extras == ["NICKNAME"]
    assert any("Missing expected columns" in msg for msg in mock_logger.messages)
    assert any("Unused columns" in msg for msg in mock_logger.messages)
