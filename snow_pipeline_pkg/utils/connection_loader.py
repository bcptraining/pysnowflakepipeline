import os
import json
import logging
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from pathlib import Path


def load_connection_config(path: str = "./config/connection_details.json") -> dict:
    log = logging.getLogger(__name__)
    config_file = Path(path).resolve()

    if not config_file.is_file():
        raise FileNotFoundError(f"❌ Config file not found at: {config_file}")
    log.info(f"🔍 Loading connection config from {config_file}")

    config = json.loads(config_file.read_text(encoding="utf-8"))

    # Resolve key path relative to config file location
    raw_key_path = config.get("private_key_path", "config/snowflake_private_key.p8")
    key_path = Path(raw_key_path)
    if not key_path.is_absolute():
        key_path = config_file.parent / key_path

    if not key_path.is_file():
        raise FileNotFoundError(f"❌ RSA key not found at: {key_path}")
    log.info(f"🔑 Loading RSA key from {key_path}")

    with key_path.open("rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend(),
        )

    pk_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    config["private_key"] = pk_bytes
    config.pop("private_key_path", None)

    log.info("✅ Connection config loaded and RSA key injected.")
    return config


# def load_connection_config(path: str = "./config/connection_details.json") -> dict:
#     """
#     Loads Snowflake connection config and injects private key bytes for key-pair authentication.

#     Reads the config JSON and private key file, then returns a config dict
#     ready for use with Snowpark's Session builder.

#     Args:
#         path (str): Path to the connection details JSON file.

#     Returns:
#         dict: Modified connection config with private key bytes added.
#     """
#     log = logging.getLogger(__name__)

#     if not os.path.exists(path):
#         raise FileNotFoundError(f"❌ Config file not found at: {path}")
#     log.info(f"🔍 Loading connection config from {path}")

#     with open(path, "r") as f:
#         config = json.load(f)

#     private_key_path = config.get(
#         "private_key_path", "./config/snowflake_private_key.p8"
#     )
#     if not os.path.exists(private_key_path):
#         raise FileNotFoundError(f"❌ RSA key not found at: {private_key_path}")
#     log.info(f"🔑 Loading RSA key from {private_key_path}")

#     with open(private_key_path, "rb") as key_file:
#         private_key = serialization.load_pem_private_key(
#             key_file.read(),
#             password=None,
#             backend=default_backend(),
#         )

#     pk_bytes = private_key.private_bytes(
#         encoding=serialization.Encoding.DER,
#         format=serialization.PrivateFormat.PKCS8,
#         encryption_algorithm=serialization.NoEncryption(),
#     )

#     config["private_key"] = pk_bytes
#     config.pop("private_key_path", None)

#     log.info("✅ Connection config loaded and RSA key injected.")
#     return config
