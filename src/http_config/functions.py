# Importing required libraries
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from snowflake.ingest import SimpleIngestManager, StagedFile
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PrivateFormat
from cryptography.hazmat.primitives.serialization import NoEncryption
from cryptography.hazmat.backends import default_backend

# Import logging
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)


# Function to get the private key passphrase from Azure Key Vault
def get_private_key_passphrase(key_vault_url, secret_name):
    try:
        # Set up Azure Key Vault client
        credential = DefaultAzureCredential()
        key_vault_client = SecretClient(vault_url=key_vault_url, credential=credential)

        # Retrieve the private key passphrase from Azure Key Vault
        private_key_passphrase = key_vault_client.get_secret(secret_name).value
        return private_key_passphrase
    except Exception as e:
        logging.error(f"Failed to get private key passphrase: {e}")
        raise


# Function to load the private key
def load_private_key(path_to_private_key, private_key_passphrase):
    try:
        with open(path_to_private_key, "rb") as pem_in:
            pemlines = pem_in.read()
            private_key_obj = load_pem_private_key(
                pemlines, private_key_passphrase.encode(), default_backend()
            )

        private_key_text = private_key_obj.private_bytes(
            Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
        ).decode("utf-8")
        return private_key_text
    except Exception as e:
        logging.error(f"Failed to load private key: {e}")
        raise


# Function to create an ingest manager
def create_ingest_manager(
    account_identifier,
    user_login_name,
    db_name,
    schema_name,
    pipe_name,
    private_key_text,
):
    try:
        ingest_manager = SimpleIngestManager(
            account=account_identifier,
            host=f"{account_identifier}.snowflakecomputing.com",
            user=user_login_name,
            pipe=f"{db_name}.{schema_name}.{pipe_name}",
            private_key=private_key_text,
        )
        return ingest_manager
    except Exception as e:
        logging.error(f"Failed to create ingest manager: {e}")
        raise


# Function to set up Azure Blob Storage client
def setup_blob_storage_client(storage_account_url):
    try:
        # Set up Azure Blob Storage client
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=storage_account_url, credential=credential
        )
        return blob_service_client
    except Exception as e:
        logging.error(f"Failed to set up blob storage client: {e}")
        raise


# Function to get the blob container client
def get_blob_container_client(blob_service_client, container_name):
    try:
        # Get the blob container client
        container_client = blob_service_client.get_container_client(container_name)
        return container_client
    except Exception as e:
        logging.error(f"Failed to get blob container client: {e}")
        raise


# Function to ingest files
def ingest_files(ingest_manager, file_paths, container_client):
    try:
        for path in file_paths:
            # Get the blobs in the path
            blob_list = container_client.list_blobs(name_starts_with=path)

            # Sort the blobs by last modified date
            sorted_blobs = sorted(
                blob_list, key=lambda blob: blob.last_modified, reverse=True
            )

            # Get the latest blob
            latest_blob = sorted_blobs[0]

            # Create a StagedFile object
            staged_file = StagedFile(latest_blob.name, None)

            # Ingest the file
            resp = ingest_manager.ingest_files([staged_file])

            # Check the response for errors
            if resp["responseCode"] == 200:
                logging.info(f"File {latest_blob.name} ingested successfully")
            else:
                logging.error(
                    "Ingestion of file {0} failed with response code: {1}".format(
                        latest_blob.name, resp["responseCode"]
                    )
                )
    except ResourceNotFoundError as e:
        logging.error(f"Failed to ingest files: {e}. The file or blob does not exist.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
