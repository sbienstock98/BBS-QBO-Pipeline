from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # QuickBooks OAuth App credentials (from Intuit Developer Portal)
    QBO_CLIENT_ID: str = ""
    QBO_CLIENT_SECRET: str = ""
    QBO_REDIRECT_URI: str = "http://localhost:8080/callback"

    # QuickBooks API
    QBO_BASE_URL: str = "https://quickbooks.api.intuit.com"
    QBO_API_VERSION: str = "v3"
    QBO_MINOR_VERSION: int = 75
    QBO_MAX_RESULTS: int = 1000
    QBO_RATE_LIMIT_PER_MIN: int = 500

    # Azure Key Vault
    AZURE_KEY_VAULT_URL: str = ""

    # Azure SQL Database
    AZURE_SQL_CONNECTION_STRING: str = ""

    # Azure Blob Storage
    AZURE_STORAGE_CONNECTION_STRING: str = ""
    AZURE_STORAGE_CONTAINER: str = "qbo-raw-data"

    # Local development overrides
    TOKEN_STORAGE: str = "local"  # "local" or "keyvault"
    LOCAL_TOKEN_DIR: str = "data/tokens"
    LOCAL_DB_PATH: str = "data/dev.db"
    DB_BACKEND: str = "sqlite"  # "sqlite" or "azure_sql"

    model_config = {"env_file": ".env", "extra": "ignore"}
