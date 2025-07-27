import os
import sys
import pytest
from snow_pipeline_pkg.pipeline_runner import parse_args  # Update path if moved


@pytest.fixture(autouse=True)
def reset_env(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_CONFIG", raising=False)
    monkeypatch.delenv("COPY_CONFIG", raising=False)


def test_default_args(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["pipeline_runner.py"])
    args = parse_args()
    assert args.connection_config == "snow_pipeline_pkg/config/connection_details.json"
    assert (
        args.copy_config
        == "snow_pipeline_pkg/config/copy_to_snowstg_avro_emp_details_avro_cls.json"
    )
    assert args.verbose is False


def test_env_override(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_CONFIG", "env_override/connection.json")
    monkeypatch.setenv("COPY_CONFIG", "env_override/copy.json")
    monkeypatch.setattr(sys, "argv", ["pipeline_runner.py"])
    args = parse_args()
    assert args.connection_config == "env_override/connection.json"
    assert args.copy_config == "env_override/copy.json"


def test_cli_override(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pipeline_runner.py",
            "--connection-config",
            "cli_override/connection.json",
            "--copy-config",
            "cli_override/copy.json",
            "--verbose",
        ],
    )
    args = parse_args()
    assert args.connection_config == "cli_override/connection.json"
    assert args.copy_config == "cli_override/copy.json"
    assert args.verbose is True
