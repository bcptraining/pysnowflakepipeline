import os
import pytest
from snow_pipeline_pkg.utils.connection_loader import load_connection_config


def test_valid_config_load(monkeypatch, tmp_path):
    # tmp_path: pytest fixture that creates a temporary folder for the test (no need to clean up manually!)
    # monkeypatch: lets us override specific function behavior — you'll see how that helps bypass cryptography in a second.

    # Create mock config file
    config_path = tmp_path / "connection_details.json"
    private_key_path = tmp_path / "snowflake_private_key.p8"

    # Minimal fake RSA key (not usable, but good enough to trigger the loader)
    dummy_key = b"-----BEGIN PRIVATE KEY-----\nMIIEv...FAKEKEY...IDAQAB\n-----END PRIVATE KEY-----"

    config_data = {
        "account": "test_account",
        "user": "test_user",
        "private_key_path": str(private_key_path),
    }
    print("config_data=", config_data)

    config_path.write_text(str(config_data).replace("'", '"'))
    print("config_path=", config_path)
    private_key_path.write_bytes(dummy_key)
    print("private_key_path=", private_key_path)

    # Monkeypatch serialization to bypass actual RSA key loading
    from snow_pipeline_pkg.utils import connection_loader

    monkeypatch.setattr(
        connection_loader.serialization,
        "load_pem_private_key",
        lambda x, password, backend: DummyKey(),
    )

    config = load_connection_config(str(config_path))
    assert "private_key" in config
    assert config["user"] == "test_user"
    assert "private_key_path" not in config


class DummyKey:
    def private_bytes(self, encoding, format, encryption_algorithm):
        return b"FAKEKEYBYTES"
