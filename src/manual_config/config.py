import os

# Azure Key Vault configurations
KEY_VAULT_URL = os.getenv("KEY_VAULT_URL")
SECRET_NAME = os.getenv("SECRET_NAME")

# Snowflake configurations
ACCOUNT_IDENTIFIER = os.getenv("ACCOUNT_IDENTIFIER")
USER_LOGIN_NAME = os.getenv("USER_LOGIN_NAME")
DB_NAME = os.getenv("DB_NAME")
SCHEMA_NAME = os.getenv("SCHEMA_NAME")
PIPE_NAME = os.getenv("PIPE_NAME")

# Path to private key
PATH_TO_PRIVATE_KEY = os.getenv("PATH_TO_PRIVATE_KEY")

# Azure Blob Storage configurations
STORAGE_ACCOUNT_URL = os.getenv("STORAGE_ACCOUNT_URL")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

# File paths to ingest
FILE_PATHS = os.getenv("FILE_PATHS").split(",")
