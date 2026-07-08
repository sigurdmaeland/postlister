from azure.identity import ClientSecretCredential
from typing import Optional
import os

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--sp-client-id", required=False)
    parser.add_argument("--sp-tenant-id", required=False)
    parser.add_argument("--env-file", required=False)
    return parser.parse_args()

try:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    _spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(_spark)
except Exception:
    dbutils = None


def _load_dotenv_if_available(args) -> None:
    """
    Load .env for local development. No-op if python-dotenv isn't installed.
    """
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(dotenv_path=args.env_file)
    except Exception:
        pass


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    val = os.getenv(name)
    return val if val is not None else default


def _require(value: Optional[str], name: str) -> str:
    if value is None or value == "":
        raise ValueError(f"Missing required configuration value: {name}")
    return value

def _get_secret(
    args,
    *,
    env_key: str,
    dbutils_scope: str = "postlister",
    dbutils_key: Optional[str] = None,
) -> str:
    """
    Secret resolution order:
      1) Databricks secrets (if dbutils available)
      2) Environment variable (typically from .env locally)
    """
    if dbutils is not None:
        key = dbutils_key or env_key
        return dbutils.secrets.get(scope=dbutils_scope, key=key)

    # Local / non-databricks
    _load_dotenv_if_available(args)
    return _require(_get_env(env_key), env_key)

def get_azure_credential() -> ClientSecretCredential:
    """
    Expects args fields:
      - client_id, tenant_id
    """
    args = parse_args()
    # If not on Databricks, allow .env to provide defaults even for args-like values.
    if dbutils is None:
        _load_dotenv_if_available(args)

    client_id = args.sp_client_id or _get_env("SP-PIPELINE-POSTLISTER-CLIENT-ID")
    tenant_id = args.sp_tenant_id or _get_env("SP-PIPELINE-POSTLISTER-TENANT-ID")

    client_secret = _get_secret(args, env_key="SP-PIPELINE-POSTLISTER-CLIENT-SECRET", dbutils_key="SP-PIPELINE-POSTLISTER-CLIENT-SECRET")

    db = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )

    return db;