"""
Databricks Unity Catalog Volume File Access
============================================
Access files stored in a Databricks Unity Catalog Volume WITHOUT using
Databricks compute. Unity Catalog enforces all ACLs via the Databricks token
(user/service principal permissions on the volume are respected).

Two access strategies are implemented:
  1. Files API  — Direct REST API to list/download/upload volume files.
                  This is the stable, recommended approach (no compute needed).
  2. Path Credential Vending — Obtain a short-lived Azure SAS token from UC
                  and then access ADLS directly. Useful when you need to stream
                  large files or integrate with Azure SDK tooling.

Prerequisites
-------------
- Databricks workspace with Unity Catalog enabled
- A Unity Catalog Volume (managed or external) containing your files
- A Databricks PAT or Service Principal OAuth token
- The principal must have READ VOLUME (and WRITE VOLUME for uploads) privilege
- For credential vending: metastore must have External Data Access enabled,
  and principal must have EXTERNAL USE LOCATION privilege

Required packages:
    pip install requests azure-storage-file-datalake python-dotenv

Configuration
-------------
Set the following environment variables (or create a .env file):
    DATABRICKS_HOST       = https://<workspace>.azuredatabricks.net
    DATABRICKS_TOKEN      = dapi...   (PAT or OAuth token)
    UC_CATALOG            = my_catalog
    UC_SCHEMA             = my_schema
    UC_VOLUME             = my_volume
    ADLS_ACCOUNT_NAME     = mystorageaccount   (for credential vending path only)
"""

import os
import sys
import json
import time
import requests
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv

# ── Azure SDK (only needed for Strategy 2: Credential Vending) ───────────────
try:
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.core.credentials import AzureSasCredential
    AZURE_SDK_AVAILABLE = True
except ImportError:
    AZURE_SDK_AVAILABLE = False

load_dotenv()

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════════════════════════════════════

class Config:
    """Load and validate all configuration from environment variables."""

    def __init__(self):
        self.host: str          = self._require("DATABRICKS_HOST").rstrip("/")
        self.token: str         = self._require("DATABRICKS_TOKEN")
        self.catalog: str       = self._require("UC_CATALOG")
        self.schema: str        = self._require("UC_SCHEMA")
        self.volume: str        = self._require("UC_VOLUME")
        # Optional — only needed for credential vending (Strategy 2)
        self.adls_account: str  = os.getenv("ADLS_ACCOUNT_NAME", "")

    @staticmethod
    def _require(key: str) -> str:
        val = os.getenv(key)
        if not val:
            raise EnvironmentError(
                f"Required environment variable '{key}' is not set. "
                f"Add it to your environment or a .env file."
            )
        return val

    @property
    def volume_path(self) -> str:
        """The canonical UC volume path prefix."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def auth_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }


# ═════════════════════════════════════════════════════════════════════════════
# Strategy 1 — Databricks Files API
# Access volume files directly via the Databricks REST API.
# UC ACLs are enforced: the token principal must have READ VOLUME privilege.
# No Databricks cluster or compute is required.
# API base: /api/2.0/fs/files  and  /api/2.0/fs/directories
# ═════════════════════════════════════════════════════════════════════════════

class DatabricksFilesAPI:
    """
    Interact with Unity Catalog Volumes via the Databricks Files REST API.
    All operations respect Unity Catalog permissions — no compute needed.
    """

    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(config.auth_headers)

    def _files_url(self, volume_relative_path: str = "") -> str:
        """Build the Files API URL for a given path inside the volume."""
        full_path = f"{self.config.volume_path}/{volume_relative_path}".rstrip("/")
        return f"{self.config.host}/api/2.0/fs/files{full_path}"

    def _dirs_url(self, volume_relative_path: str = "") -> str:
        """Build the Directories API URL."""
        full_path = f"{self.config.volume_path}/{volume_relative_path}".rstrip("/")
        return f"{self.config.host}/api/2.0/fs/directories{full_path}"

    def _raise_for_status(self, resp: requests.Response, context: str):
        """Raise a descriptive error if the API call failed."""
        if resp.status_code == 403:
            raise PermissionError(
                f"{context}: Access denied (HTTP 403). "
                f"Ensure your principal has READ VOLUME (or WRITE VOLUME) "
                f"privilege on '{self.config.volume_path}'."
            )
        if resp.status_code == 404:
            raise FileNotFoundError(f"{context}: Path not found (HTTP 404).")
        resp.raise_for_status()

    # ── Directory operations ──────────────────────────────────────────────────

    def list_directory(self, volume_relative_path: str = "") -> list[dict]:
        """
        List contents of a directory inside the volume.

        Args:
            volume_relative_path: Path inside the volume (e.g. "images/2024").
                                   Leave empty to list the volume root.

        Returns:
            List of dicts with keys: name, path, is_directory, file_size,
            last_modified.
        """
        url = self._dirs_url(volume_relative_path)
        log.info("Listing directory: %s/%s", self.config.volume_path, volume_relative_path)

        resp = self.session.get(url)
        self._raise_for_status(resp, f"list_directory({volume_relative_path})")

        data = resp.json()
        contents = data.get("contents", [])
        log.info("Found %d items", len(contents))
        return contents

    def list_files_recursive(
        self, volume_relative_path: str = "", extension_filter: Optional[str] = None
    ) -> list[dict]:
        """
        Recursively list all files under a directory.

        Args:
            volume_relative_path: Starting directory inside the volume.
            extension_filter: Optional file extension to filter by (e.g. ".pdf").

        Returns:
            Flat list of file metadata dicts.
        """
        all_files = []
        queue = [volume_relative_path]

        while queue:
            current = queue.pop()
            items = self.list_directory(current)
            for item in items:
                item_name = item.get("name", "")
                item_path = f"{current}/{item_name}".lstrip("/")
                if item.get("is_directory"):
                    queue.append(item_path)
                else:
                    if extension_filter is None or item_name.lower().endswith(extension_filter.lower()):
                        all_files.append({**item, "_relative_path": item_path})

        log.info("Total files found: %d", len(all_files))
        return all_files

    def create_directory(self, volume_relative_path: str) -> None:
        """
        Create a directory inside the volume.
        Requires WRITE VOLUME privilege.
        """
        url = self._dirs_url(volume_relative_path) + "/"
        log.info("Creating directory: %s/%s", self.config.volume_path, volume_relative_path)
        resp = self.session.put(url)
        self._raise_for_status(resp, f"create_directory({volume_relative_path})")
        log.info("Directory created successfully.")

    # ── File download ─────────────────────────────────────────────────────────

    def download_file(
        self,
        volume_relative_path: str,
        local_destination: Optional[str] = None,
        chunk_size: int = 8 * 1024 * 1024,  # 8 MB chunks
    ) -> bytes:
        """
        Download a file from the Unity Catalog volume.
        UC ACLs are enforced — the caller must have READ VOLUME privilege.

        Args:
            volume_relative_path: Path to the file inside the volume
                                   (e.g. "reports/2024/annual.pdf").
            local_destination: If provided, saves the file to this local path.
            chunk_size: Streaming chunk size in bytes.

        Returns:
            Raw file bytes (also saved to local_destination if specified).
        """
        url = self._files_url(volume_relative_path)
        log.info("Downloading: %s/%s", self.config.volume_path, volume_relative_path)

        # Use streaming to handle large files (images, PDFs, etc.)
        resp = self.session.get(url, headers={"Content-Type": "application/octet-stream"}, stream=True)
        self._raise_for_status(resp, f"download_file({volume_relative_path})")

        content_length = resp.headers.get("Content-Length")
        if content_length:
            log.info("File size: %.2f MB", int(content_length) / (1024 * 1024))

        chunks = []
        downloaded = 0
        for chunk in resp.iter_content(chunk_size=chunk_size):
            if chunk:
                chunks.append(chunk)
                downloaded += len(chunk)

        file_bytes = b"".join(chunks)
        log.info("Downloaded %.2f MB successfully.", len(file_bytes) / (1024 * 1024))

        if local_destination:
            dest_path = Path(local_destination)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(file_bytes)
            log.info("Saved to: %s", dest_path)

        return file_bytes

    def download_directory(self, volume_relative_path: str, local_dest_dir: str) -> list[str]:
        """
        Download all files from a volume directory to a local folder.

        Args:
            volume_relative_path: Directory inside the volume.
            local_dest_dir: Local directory to save files into.

        Returns:
            List of local file paths that were downloaded.
        """
        files = self.list_files_recursive(volume_relative_path)
        saved_paths = []

        for file_info in files:
            rel = file_info["_relative_path"]
            local_path = os.path.join(local_dest_dir, rel.lstrip("/"))
            self.download_file(rel, local_destination=local_path)
            saved_paths.append(local_path)

        log.info("Downloaded %d files to '%s'.", len(saved_paths), local_dest_dir)
        return saved_paths

    # ── File upload ───────────────────────────────────────────────────────────

    def upload_file(
        self,
        local_file_path: str,
        volume_relative_path: str,
        overwrite: bool = True,
    ) -> None:
        """
        Upload a local file to the Unity Catalog volume.
        Requires WRITE VOLUME privilege.

        Args:
            local_file_path: Path to the local file to upload.
            volume_relative_path: Destination path inside the volume
                                   (e.g. "uploads/myfile.pdf").
            overwrite: Whether to overwrite an existing file (default: True).
        """
        local_path = Path(local_file_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_file_path}")

        url = self._files_url(volume_relative_path)
        if overwrite:
            url += "?overwrite=true"

        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        log.info(
            "Uploading '%s' (%.2f MB) → %s/%s",
            local_file_path, file_size_mb,
            self.config.volume_path, volume_relative_path
        )

        with open(local_file_path, "rb") as f:
            resp = self.session.put(
                url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
            )
        self._raise_for_status(resp, f"upload_file({volume_relative_path})")
        log.info("Upload complete.")

    def upload_bytes(
        self,
        data: bytes,
        volume_relative_path: str,
        overwrite: bool = True,
    ) -> None:
        """Upload raw bytes to the volume (e.g. generated in-memory content)."""
        url = self._files_url(volume_relative_path)
        if overwrite:
            url += "?overwrite=true"

        resp = self.session.put(
            url,
            data=data,
            headers={"Content-Type": "application/octet-stream"},
        )
        self._raise_for_status(resp, f"upload_bytes({volume_relative_path})")
        log.info("Uploaded %d bytes to %s", len(data), volume_relative_path)

    # ── File delete ───────────────────────────────────────────────────────────

    def delete_file(self, volume_relative_path: str) -> None:
        """
        Delete a file from the volume.
        Requires WRITE VOLUME privilege.
        """
        url = self._files_url(volume_relative_path)
        log.info("Deleting: %s/%s", self.config.volume_path, volume_relative_path)
        resp = self.session.delete(url)
        self._raise_for_status(resp, f"delete_file({volume_relative_path})")
        log.info("File deleted.")

    # ── Volume metadata ───────────────────────────────────────────────────────

    def get_volume_info(self) -> dict:
        """Fetch metadata for the Unity Catalog volume via the UC REST API."""
        url = (
            f"{self.config.host}/api/2.1/unity-catalog/volumes/"
            f"{self.config.catalog}.{self.config.schema}.{self.config.volume}"
        )
        resp = self.session.get(url)
        self._raise_for_status(resp, "get_volume_info")
        return resp.json()


# ═════════════════════════════════════════════════════════════════════════════
# Strategy 2 — Unity Catalog Path Credential Vending + Azure ADLS Direct Access
# Obtain a short-lived Azure SAS token from Unity Catalog, then use it to
# access ADLS Gen2 directly — without any Databricks compute.
#
# Prerequisites:
#   - Metastore must have External Data Access enabled
#   - Principal needs EXTERNAL USE LOCATION on the external location
#   - ADLS_ACCOUNT_NAME env var must be set
#   - azure-storage-file-datalake package must be installed
# ═════════════════════════════════════════════════════════════════════════════

class UCCredentialVending:
    """
    Use Unity Catalog Credential Vending to get short-lived Azure SAS tokens
    and then access ADLS Gen2 directly (no Databricks compute needed).
    """

    CREDENTIAL_VENDING_ENDPOINT = "/api/2.1/unity-catalog/temporary-path-credentials"

    def __init__(self, config: Config):
        if not AZURE_SDK_AVAILABLE:
            raise ImportError(
                "azure-storage-file-datalake is required for credential vending. "
                "Install it with: pip install azure-storage-file-datalake"
            )
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(config.auth_headers)
        self._credential_cache: Optional[dict] = None
        self._credential_expiry: float = 0.0

    def _get_volume_storage_location(self) -> str:
        """
        Look up the ADLS storage path backing the UC volume via the UC REST API.
        Returns a path like: abfss://container@account.dfs.core.windows.net/path/
        """
        url = (
            f"{self.config.host}/api/2.1/unity-catalog/volumes/"
            f"{self.config.catalog}.{self.config.schema}.{self.config.volume}"
        )
        resp = self.session.get(url)
        if resp.status_code == 403:
            raise PermissionError(
                "Cannot read volume metadata. Ensure your principal has "
                "READ VOLUME privilege and EXTERNAL USE LOCATION on the external location."
            )
        resp.raise_for_status()
        volume_info = resp.json()
        storage_location = volume_info.get("storage_location")
        if not storage_location:
            raise ValueError(
                "Volume has no storage_location. "
                "Credential vending works only with external volumes backed by ADLS."
            )
        log.info("Volume ADLS storage location: %s", storage_location)
        return storage_location

    def get_temporary_credentials(
        self,
        adls_path: str,
        operation: str = "READ",
        force_refresh: bool = False,
    ) -> dict:
        """
        Request a short-lived SAS token from Unity Catalog for a given ADLS path.
        Credentials are cached until 60 seconds before expiry.

        Args:
            adls_path: The ADLS path to request credentials for
                        (abfss://container@account.dfs.core.windows.net/path/).
            operation: "READ", "READ_WRITE", or "WRITE".
            force_refresh: Force a new credential even if cached one is valid.

        Returns:
            Dict containing the vended credential details including the SAS token.
        """
        now = time.time()
        # Return cached credential if still valid (with 60-second buffer)
        if (
            not force_refresh
            and self._credential_cache
            and now < self._credential_expiry - 60
        ):
            log.debug("Using cached vended credential (expires in %.0fs).",
                      self._credential_expiry - now)
            return self._credential_cache

        url = f"{self.config.host}{self.CREDENTIAL_VENDING_ENDPOINT}"
        payload = {
            "url": adls_path,
            "operation": operation,
        }

        log.info("Requesting %s credential from Unity Catalog for: %s", operation, adls_path)
        resp = self.session.post(url, json=payload)

        if resp.status_code == 403:
            raise PermissionError(
                f"Credential vending denied (HTTP 403). Check that:\n"
                f"  1. External Data Access is enabled on the metastore\n"
                f"  2. Principal has EXTERNAL USE LOCATION on the external location\n"
                f"  3. Operation '{operation}' is permitted for this principal\n"
                f"  Response: {resp.text}"
            )
        if resp.status_code == 400:
            raise ValueError(
                f"Credential vending request error: {resp.text}\n"
                f"Ensure the volume is an external volume backed by ADLS."
            )
        resp.raise_for_status()

        creds = resp.json()
        log.info("Temporary credential vended successfully.")

        # Parse and cache expiry time
        expiry_str = creds.get("expiration_time") or creds.get("azure", {}).get("sas_token_expiry")
        if expiry_str:
            try:
                expiry_dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
                self._credential_expiry = expiry_dt.timestamp()
                log.info("Credential valid until: %s", expiry_dt.isoformat())
            except Exception:
                self._credential_expiry = now + 3600  # Assume 1 hour if unparseable

        self._credential_cache = creds
        return creds

    def _build_adls_client(self, creds: dict) -> DataLakeServiceClient:
        """
        Build an Azure DataLakeServiceClient using vended SAS credentials.
        """
        # UC returns Azure delegation SAS credentials
        azure_creds = creds.get("azure_user_delegation_sas") or creds.get("azure", {})
        sas_token = azure_creds.get("sas_token") or azure_creds.get("token")

        if not sas_token:
            raise ValueError(
                f"No SAS token found in vended credentials. "
                f"Response keys: {list(creds.keys())}"
            )

        account_url = f"https://{self.config.adls_account}.dfs.core.windows.net"
        return DataLakeServiceClient(
            account_url=account_url,
            credential=AzureSasCredential(sas_token),
        )

    def _parse_adls_path(self, adls_abfss_path: str) -> tuple[str, str]:
        """
        Parse an abfss:// path into (container_name, file_path).
        e.g. abfss://mycontainer@account.dfs.core.windows.net/data/file.pdf
             → ("mycontainer", "data/file.pdf")
        """
        # Strip scheme
        path = adls_abfss_path.replace("abfss://", "")
        container, rest = path.split("@", 1)
        _, file_path = rest.split("/", 1)
        return container, file_path

    def download_file_via_adls(
        self,
        volume_relative_path: str,
        local_destination: Optional[str] = None,
    ) -> bytes:
        """
        Download a file from the ADLS path backing a UC volume,
        using a vended SAS token. No Databricks compute required.

        Args:
            volume_relative_path: Path inside the volume (e.g. "images/photo.jpg").
            local_destination: Optional local path to save the file.

        Returns:
            Raw file bytes.
        """
        storage_location = self._get_volume_storage_location()
        full_adls_path = f"{storage_location.rstrip('/')}/{volume_relative_path}"

        creds = self.get_temporary_credentials(storage_location, operation="READ")
        client = self._build_adls_client(creds)

        container, file_path = self._parse_adls_path(full_adls_path)
        log.info("Downloading via ADLS: container=%s path=%s", container, file_path)

        fs_client = client.get_file_system_client(container)
        file_client = fs_client.get_file_client(file_path)

        download = file_client.download_file()
        file_bytes = download.readall()
        log.info("Downloaded %d bytes via ADLS.", len(file_bytes))

        if local_destination:
            dest = Path(local_destination)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(file_bytes)
            log.info("Saved to: %s", dest)

        return file_bytes

    def list_files_via_adls(self, volume_relative_path: str = "") -> list[dict]:
        """
        List files in the ADLS path backing the UC volume using vended credentials.
        """
        storage_location = self._get_volume_storage_location()
        creds = self.get_temporary_credentials(storage_location, operation="READ")
        client = self._build_adls_client(creds)

        base_path = storage_location.rstrip("/")
        if volume_relative_path:
            list_path = f"{base_path}/{volume_relative_path}"
        else:
            list_path = base_path

        container, dir_path = self._parse_adls_path(list_path)
        log.info("Listing via ADLS: container=%s path=%s", container, dir_path)

        fs_client = client.get_file_system_client(container)
        paths = fs_client.get_paths(path=dir_path)

        results = []
        for p in paths:
            results.append({
                "name": p.name.split("/")[-1],
                "full_adls_path": f"abfss://{container}@{self.config.adls_account}.dfs.core.windows.net/{p.name}",
                "is_directory": p.is_directory,
                "size_bytes": p.content_length,
                "last_modified": str(p.last_modified),
            })

        log.info("Found %d items.", len(results))
        return results


# ═════════════════════════════════════════════════════════════════════════════
# Helper utilities
# ═════════════════════════════════════════════════════════════════════════════

def format_file_listing(items: list[dict]) -> str:
    """Pretty-print a file listing."""
    lines = []
    for item in items:
        name = item.get("name", item.get("_relative_path", "?"))
        is_dir = item.get("is_directory", False)
        size = item.get("file_size", item.get("size_bytes", 0)) or 0
        icon = "📁" if is_dir else "📄"
        size_str = f"{size / 1024:.1f} KB" if size else ""
        lines.append(f"  {icon}  {name:<50} {size_str}")
    return "\n".join(lines)


def check_volume_permissions(config: Config) -> dict:
    """
    Check what permissions the current principal has on the volume.
    Returns volume metadata including volume_type and storage_location.
    """
    session = requests.Session()
    session.headers.update(config.auth_headers)

    url = (
        f"{config.host}/api/2.1/unity-catalog/volumes/"
        f"{config.catalog}.{config.schema}.{config.volume}"
    )
    resp = session.get(url)

    if resp.status_code == 403:
        return {"error": "No access to this volume (HTTP 403)."}
    if resp.status_code == 404:
        return {"error": f"Volume '{config.volume_path}' not found (HTTP 404)."}

    resp.raise_for_status()
    return resp.json()


# ═════════════════════════════════════════════════════════════════════════════
# Main demonstration
# ═════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 65)
    print("  Databricks Unity Catalog Volume Access — No Compute Required")
    print("=" * 65 + "\n")

    # ── Load configuration ────────────────────────────────────────────────────
    try:
        config = Config()
    except EnvironmentError as e:
        log.error("Configuration error: %s", e)
        sys.exit(1)

    log.info("Target volume: %s", config.volume_path)

    # ── Pre-flight: check volume exists and is accessible ─────────────────────
    log.info("Checking volume permissions...")
    volume_info = check_volume_permissions(config)
    if "error" in volume_info:
        log.error(volume_info["error"])
        sys.exit(1)

    volume_type = volume_info.get("volume_type", "UNKNOWN")
    storage_loc = volume_info.get("storage_location", "N/A")
    log.info("Volume type: %s", volume_type)
    log.info("Storage location: %s", storage_loc)

    # ═════════════════════════════════════════════════════════════════════════
    # STRATEGY 1: Databricks Files API (recommended for all volume types)
    # ═════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 50)
    print("  Strategy 1: Databricks Files API")
    print("─" * 50)

    files_api = DatabricksFilesAPI(config)

    # List the root of the volume
    print(f"\n📂 Contents of {config.volume_path}:")
    try:
        items = files_api.list_directory()
        print(format_file_listing(items))
    except PermissionError as e:
        log.error("Permission denied listing volume root: %s", e)

    # List all PDFs recursively
    print("\n📄 All PDF files (recursive):")
    try:
        pdf_files = files_api.list_files_recursive(extension_filter=".pdf")
        for f in pdf_files:
            print(f"   {f['_relative_path']}")
        print(f"   Total: {len(pdf_files)} PDFs")
    except PermissionError as e:
        log.error("%s", e)

    # Example: Download a specific file
    # Uncomment and set the path to a real file in your volume
    #
    # DOWNLOAD_PATH = "documents/report.pdf"
    # try:
    #     content = files_api.download_file(
    #         volume_relative_path=DOWNLOAD_PATH,
    #         local_destination=f"./downloads/{DOWNLOAD_PATH.split('/')[-1]}",
    #     )
    #     print(f"\n✅ Downloaded {len(content):,} bytes from {DOWNLOAD_PATH}")
    # except (PermissionError, FileNotFoundError) as e:
    #     log.error("Download failed: %s", e)

    # Example: Upload a file
    # Uncomment to test uploads (requires WRITE VOLUME privilege)
    #
    # try:
    #     files_api.upload_file(
    #         local_file_path="./myfile.pdf",
    #         volume_relative_path="uploads/myfile.pdf",
    #     )
    # except PermissionError as e:
    #     log.error("Upload failed (needs WRITE VOLUME privilege): %s", e)

    # Example: Create a directory
    # try:
    #     files_api.create_directory("uploads/2024")
    # except PermissionError as e:
    #     log.error("Create directory failed: %s", e)

    # ═════════════════════════════════════════════════════════════════════════
    # STRATEGY 2: UC Credential Vending → Direct ADLS Access
    # Only available for external volumes backed by ADLS.
    # Requires: External Data Access enabled on metastore,
    #           EXTERNAL USE LOCATION privilege, ADLS_ACCOUNT_NAME env var.
    # ═════════════════════════════════════════════════════════════════════════
    if volume_type == "EXTERNAL" and config.adls_account and AZURE_SDK_AVAILABLE:
        print("\n" + "─" * 50)
        print("  Strategy 2: UC Credential Vending → ADLS Direct Access")
        print("─" * 50)

        cred_vending = UCCredentialVending(config)

        try:
            print("\n🔑 Requesting temporary SAS token from Unity Catalog...")
            storage_location = volume_info.get("storage_location", "")
            creds = cred_vending.get_temporary_credentials(
                adls_path=storage_location,
                operation="READ",
            )
            print(f"✅ Temporary credential obtained. Expires: "
                  f"{creds.get('expiration_time', 'N/A')}")

            print("\n📂 Listing volume via ADLS direct access:")
            adls_items = cred_vending.list_files_via_adls()
            print(format_file_listing(adls_items))

            # Example: Download a file via ADLS
            # Uncomment and set the path to a real file in your volume
            #
            # DOWNLOAD_PATH = "images/photo.jpg"
            # content = cred_vending.download_file_via_adls(
            #     volume_relative_path=DOWNLOAD_PATH,
            #     local_destination=f"./downloads/{DOWNLOAD_PATH.split('/')[-1]}",
            # )
            # print(f"\n✅ Downloaded {len(content):,} bytes via ADLS.")

        except PermissionError as e:
            log.warning("Credential vending not available: %s", e)
            log.info("Strategy 1 (Files API) remains available and fully functional.")
        except Exception as e:
            log.warning("Credential vending error: %s", e)

    elif volume_type == "MANAGED":
        print("\n  ℹ️  Strategy 2 (Credential Vending) is not available for MANAGED volumes.")
        print("     Use Strategy 1 (Files API) — it works fully for managed volumes.")
    elif not config.adls_account:
        print("\n  ℹ️  Set ADLS_ACCOUNT_NAME to enable Strategy 2 (Credential Vending).")

    print("\n" + "=" * 65)
    print("  Done.")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
