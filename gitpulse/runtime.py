from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from dotenv import load_dotenv

StorageBackend = Literal["local", "s3"]


class GitPulseRuntimeError(RuntimeError):
    """Base class for pipeline runtime errors."""


class WatermarkReadError(GitPulseRuntimeError):
    """Raised when sync state exists but cannot be read safely."""


def configure_logging() -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


@dataclass(frozen=True)
class RuntimeConfig:
    storage_backend: StorageBackend
    duckdb_file: str
    bucket: str | None = None
    endpoint: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    data_root: str | None = None

    @classmethod
    def from_env(cls) -> "RuntimeConfig":
        load_dotenv()
        storage_backend = os.environ.get("GITPULSE_STORAGE_BACKEND")
        data_root = os.environ.get("GITPULSE_LOCAL_DATA_ROOT")
        if storage_backend is None:
            storage_backend = "local" if data_root else "s3"

        if storage_backend not in {"local", "s3"}:
            raise ValueError(
                "GITPULSE_STORAGE_BACKEND must be 'local' or 's3', "
                f"got {storage_backend!r}"
            )

        if storage_backend == "local" and not data_root:
            raise ValueError(
                "GITPULSE_LOCAL_DATA_ROOT must be set when GITPULSE_STORAGE_BACKEND=local"
            )

        return cls(
            storage_backend=storage_backend,
            duckdb_file=os.environ.get("DUCKDB_FILE", "/tmp/gitpulse.duckdb"),
            bucket=os.environ.get("MINIO_BUCKET", "gitpulse"),
            endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.environ.get("MINIO_ACCESS_KEY", "gitpulse"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", "gitpulse"),
            data_root=data_root,
        )

    @property
    def is_local(self) -> bool:
        return self.storage_backend == "local"

    def resolve(self, relative_path: str) -> str:
        if self.is_local:
            return str((Path(self.data_root or "").resolve() / relative_path).resolve())
        return f"s3://{self.bucket}/{relative_path}"

    def local_path(self, relative_path: str) -> Path:
        if not self.is_local:
            raise ValueError("local_path() is only valid for local storage")
        return (Path(self.data_root or "").resolve() / relative_path).resolve()

    def ensure_parent_dir(self, relative_path: str) -> None:
        if self.is_local:
            self.local_path(relative_path).parent.mkdir(parents=True, exist_ok=True)

    def make_s3_client(self):
        return boto3.client(
            "s3",
            endpoint_url=f"http://{self.endpoint}",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=BotoConfig(signature_version="s3v4"),
            region_name="us-east-1",
        )

    def exists(self, relative_path: str) -> bool:
        if self.is_local:
            return self.local_path(relative_path).exists()

        assert self.bucket is not None
        client = self.make_s3_client()
        try:
            client.head_object(Bucket=self.bucket, Key=relative_path)
            return True
        except ClientError as exc:
            status_code = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code == 404:
                return False
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def list_relative_paths(self, prefix: str, suffix: str | None = None) -> list[str]:
        if self.is_local:
            base = self.local_path(prefix)
            if not base.exists():
                return []
            paths: list[Path]
            if base.is_file():
                paths = [base]
            else:
                paths = [p for p in base.rglob("*") if p.is_file()]
            relatives = [
                p.relative_to(self.local_path("")).as_posix()
                for p in paths
                if suffix is None or p.as_posix().endswith(suffix)
            ]
            return sorted(relatives)

        assert self.bucket is not None
        client = self.make_s3_client()
        paginator = client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if suffix is None or key.endswith(suffix):
                    keys.append(key)
        return sorted(keys)

    def write_small_parquet(self, relative_path: str, records: list[dict]) -> None:
        if not records:
            raise ValueError("write_small_parquet requires at least one record")

        table = pa.Table.from_pylist(records)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        if self.is_local:
            self.ensure_parent_dir(relative_path)
            self.local_path(relative_path).write_bytes(buf.getvalue())
            return

        assert self.bucket is not None
        client = self.make_s3_client()
        client.put_object(Bucket=self.bucket, Key=relative_path, Body=buf.getvalue())

    def overwrite_small_parquet(self, relative_path: str, records: list[dict]) -> None:
        self.write_small_parquet(relative_path, records)

    def append_small_parquet(self, relative_path: str, records: list[dict]) -> None:
        existing = self.read_small_parquet(relative_path) if self.exists(relative_path) else []
        self.write_small_parquet(relative_path, [*existing, *records])

    def read_small_parquet(self, relative_path: str) -> list[dict]:
        if self.is_local:
            path = self.local_path(relative_path)
            if not path.exists():
                raise FileNotFoundError(path)
            return pq.ParquetFile(path).read().to_pylist()

        assert self.bucket is not None
        client = self.make_s3_client()
        try:
            body = client.get_object(Bucket=self.bucket, Key=relative_path)["Body"].read()
        except ClientError as exc:
            status_code = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code == 404:
                raise FileNotFoundError(relative_path) from exc
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(relative_path) from exc
            raise
        return pq.read_table(io.BytesIO(body)).to_pylist()


def configure_duckdb(conn: duckdb.DuckDBPyConnection, config: RuntimeConfig) -> None:
    if config.is_local:
        return

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(
        f"""
        CREATE OR REPLACE SECRET minio (
            TYPE S3,
            KEY_ID '{sql_literal(config.access_key or "")}',
            SECRET '{sql_literal(config.secret_key or "")}',
            ENDPOINT '{sql_literal(config.endpoint or "")}',
            USE_SSL false,
            URL_STYLE 'path'
        );
        """
    )


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def bronze_repo_prefix(repo_owner: str, repo_name: str) -> str:
    return f"bronze/github/issues/repo_owner={repo_owner}/repo_name={repo_name}"


def bronze_run_prefix(repo_owner: str, repo_name: str, run_id: str) -> str:
    return f"{bronze_repo_prefix(repo_owner, repo_name)}/run_id={run_id}"


def bronze_page_relative_path(repo_owner: str, repo_name: str, run_id: str, page_number: int) -> str:
    return f"{bronze_run_prefix(repo_owner, repo_name, run_id)}/page_{page_number:03d}.parquet"


def bronze_manifest_relative_path(repo_owner: str, repo_name: str) -> str:
    return (
        "bronze/github/issues/manifests/"
        f"repo_owner={repo_owner}/repo_name={repo_name}/data.parquet"
    )


def silver_dataset_relative_path(dataset: str, repo_owner: str, repo_name: str) -> str:
    return f"silver/{dataset}/repo_owner={repo_owner}/repo_name={repo_name}/data.parquet"


def gold_dataset_relative_path(dataset: str, repo_owner: str, repo_name: str) -> str:
    return f"gold/{dataset}/repo_owner={repo_owner}/repo_name={repo_name}/data.parquet"


def load_manifest_rows(
    config: RuntimeConfig,
    repo_owner: str,
    repo_name: str,
) -> list[dict]:
    path = bronze_manifest_relative_path(repo_owner, repo_name)
    if not config.exists(path):
        return []
    rows = config.read_small_parquet(path)
    rows.sort(key=lambda row: (str(row.get("finished_at") or ""), str(row.get("run_id") or "")))
    return rows


def get_latest_successful_manifest(
    config: RuntimeConfig,
    repo_owner: str,
    repo_name: str,
) -> dict | None:
    rows = [row for row in load_manifest_rows(config, repo_owner, repo_name) if row.get("status") == "success"]
    if not rows:
        return None
    rows.sort(
        key=lambda row: (str(row.get("finished_at") or ""), str(row.get("run_id") or "")),
        reverse=True,
    )
    return rows[0]
