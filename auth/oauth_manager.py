import json
import logging
import os
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

TOKEN_ENDPOINT = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

# Refresh access token 5 minutes before expiry
TOKEN_REFRESH_BUFFER_SECONDS = 300

# Alert when refresh token is older than 90 days (expires at 101)
REFRESH_TOKEN_WARN_DAYS = 90


class OAuthManager:
    """Manages QBO OAuth2 tokens with Key Vault or local file storage."""

    def __init__(self, settings, client_id: str):
        self.settings = settings
        self.client_key = client_id
        self._storage = self._init_storage()

    def _init_storage(self):
        if self.settings.TOKEN_STORAGE == "keyvault":
            return KeyVaultTokenStorage(
                self.settings.AZURE_KEY_VAULT_URL, self.client_key
            )
        return LocalTokenStorage(self.settings.LOCAL_TOKEN_DIR, self.client_key)

    def get_valid_access_token(self) -> str:
        """Return a valid access token, refreshing if expired."""
        token_data = self._storage.load()
        expiry = token_data.get("token_expiry", 0)

        if time.time() > expiry - TOKEN_REFRESH_BUFFER_SECONDS:
            logger.info(f"Access token expired for {self.client_key}, refreshing...")
            self._refresh_token(token_data)
            token_data = self._storage.load()

        self._check_refresh_token_age(token_data)
        return token_data["access_token"]

    def get_realm_id(self) -> str:
        """Return the QBO realm ID (company ID) for this client."""
        token_data = self._storage.load()
        return token_data["realm_id"]

    def store_initial_tokens(
        self,
        access_token: str,
        refresh_token: str,
        realm_id: str,
        expires_in: int = 3600,
    ):
        """Store tokens from initial OAuth authorization flow."""
        self._storage.save(
            {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "realm_id": realm_id,
                "token_expiry": time.time() + expires_in,
                "refresh_token_issued": time.time(),
            }
        )
        logger.info(f"Stored initial tokens for {self.client_key}")

    def _refresh_token(self, token_data: dict):
        """Exchange refresh token for new access + refresh tokens."""
        refresh_token = token_data["refresh_token"]

        response = requests.post(
            TOKEN_ENDPOINT,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            auth=(self.settings.QBO_CLIENT_ID, self.settings.QBO_CLIENT_SECRET),
            timeout=30,
        )
        response.raise_for_status()
        tokens = response.json()

        token_data["access_token"] = tokens["access_token"]
        token_data["refresh_token"] = tokens["refresh_token"]
        token_data["token_expiry"] = time.time() + tokens["expires_in"]
        token_data["refresh_token_issued"] = time.time()
        self._storage.save(token_data)

        logger.info(f"Refreshed tokens for {self.client_key}")

    def _check_refresh_token_age(self, token_data: dict):
        """Warn if refresh token is approaching its 101-day expiry."""
        issued = token_data.get("refresh_token_issued", 0)
        if issued == 0:
            return
        age_days = (time.time() - issued) / 86400
        if age_days > REFRESH_TOKEN_WARN_DAYS:
            logger.warning(
                f"Refresh token for {self.client_key} is {age_days:.0f} days old. "
                f"It expires at 101 days. Re-authorize soon!"
            )


class LocalTokenStorage:
    """Store OAuth tokens as JSON files on disk (for development)."""

    def __init__(self, token_dir: str, client_key: str):
        self.token_dir = Path(token_dir)
        self.token_dir.mkdir(parents=True, exist_ok=True)
        self.token_file = self.token_dir / f"{client_key}.json"

    def load(self) -> dict:
        if not self.token_file.exists():
            raise FileNotFoundError(
                f"No tokens found for client. Run onboard_client.py first. "
                f"Expected: {self.token_file}"
            )
        with open(self.token_file) as f:
            return json.load(f)

    def save(self, token_data: dict):
        with open(self.token_file, "w") as f:
            json.dump(token_data, f, indent=2)


class KeyVaultTokenStorage:
    """Store OAuth tokens in Azure Key Vault (for production)."""

    def __init__(self, vault_url: str, client_key: str):
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        self.client = SecretClient(
            vault_url=vault_url, credential=DefaultAzureCredential()
        )
        self.prefix = client_key

    def _secret_name(self, key: str) -> str:
        # Key Vault secret names: alphanumeric and hyphens only
        return f"{self.prefix}-{key}".replace("_", "-")

    def load(self) -> dict:
        try:
            secret = self.client.get_secret(self._secret_name("token-data"))
            return json.loads(secret.value)
        except Exception as e:
            raise FileNotFoundError(
                f"No tokens in Key Vault for {self.prefix}: {e}"
            )

    def save(self, token_data: dict):
        self.client.set_secret(
            self._secret_name("token-data"), json.dumps(token_data)
        )
