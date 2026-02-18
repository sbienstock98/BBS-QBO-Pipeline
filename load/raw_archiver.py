"""Archive raw QBO API JSON responses for auditability and replay."""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def archive_raw_json(
    settings,
    client_id: str,
    source_name: str,
    data: object,
):
    """Save raw API response as JSON.

    In local dev, saves to disk. In production, uploads to Azure Blob Storage.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    blob_path = f"{client_id}/{source_name}/{timestamp}.json"

    json_bytes = json.dumps(data, default=str, indent=2).encode("utf-8")

    if settings.TOKEN_STORAGE == "keyvault":
        _archive_to_blob(settings, blob_path, json_bytes)
    else:
        _archive_to_local(settings, blob_path, json_bytes)


def _archive_to_local(settings, blob_path: str, data: bytes):
    """Save raw JSON to local filesystem."""
    base = Path("data/raw_archive")
    file_path = base / blob_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(data)
    logger.debug(f"Archived to {file_path}")


def _archive_to_blob(settings, blob_path: str, data: bytes):
    """Upload raw JSON to Azure Blob Storage."""
    from azure.storage.blob import BlobServiceClient

    blob_service = BlobServiceClient.from_connection_string(
        settings.AZURE_STORAGE_CONNECTION_STRING
    )
    container_client = blob_service.get_container_client(
        settings.AZURE_STORAGE_CONTAINER
    )

    # Create container if it doesn't exist
    try:
        container_client.create_container()
    except Exception:
        pass  # Container already exists

    blob_client = container_client.get_blob_client(blob_path)
    blob_client.upload_blob(data, overwrite=True)
    logger.debug(f"Archived to blob: {blob_path}")
