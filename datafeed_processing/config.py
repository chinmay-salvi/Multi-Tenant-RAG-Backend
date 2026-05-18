"""
Environment-based settings for datafeed_processing workers.

Set variables in the shell, a .env loader, or your container orchestrator.
Do not commit secrets to the repository.
"""

from __future__ import annotations

import json
import os
from typing import Any


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


DATABASE_URL = _env("DATABASE_URL", "")

AWS_ACCESS_KEY_ID = _env("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = _env("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = _env("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = _env("S3_BUCKET_NAME", "")

VECTOR_STORE_TABLE_NAME = _env("VECTOR_STORE_TABLE_NAME", "pg_vector_store")
NODE_PARSER_CHUNK_SIZE = int(_env("NODE_PARSER_CHUNK_SIZE", "512"))
NODE_PARSER_CHUNK_OVERLAP = int(_env("NODE_PARSER_CHUNK_OVERLAP", "10"))
CLOUDFLARE_EMBEDDING_MODEL_NAME = _env(
    "CLOUDFLARE_EMBEDDING_MODEL_NAME", "@cf/baai/bge-base-en-v1.5"
)
CLOUDFLARE_EMBEDDING_MODEL_DIMENSION = int(
    _env("CLOUDFLARE_EMBEDDING_MODEL_DIMENSION", "768")
)

LOG_LEVEL = _env("LOG_LEVEL", "INFO")

# Used by plans_helper (Supabase)
SPB_CONN_URL = _env("SUPABASE_URL", "")
SPB_KEY = _env("SUPABASE_KEY", "")


def load_auth_cred_dict(record: str | None) -> dict[str, Any]:
    """
    Parse SysAuthCred.authCred as a JSON object (same shape as before, but JSON not Python repr).

    Raises json.JSONDecodeError, ValueError, or TypeError if invalid.
    """
    if record is None or not str(record).strip():
        raise ValueError("empty auth credential")
    data = json.loads(record)
    if not isinstance(data, dict):
        raise ValueError("auth cred JSON must be an object")
    return data


def s3_client_credentials() -> dict[str, str]:
    """Keyword args for aiobotocore S3 client; empty dict uses default AWS credential chain."""
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        return {
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        }
    return {}
