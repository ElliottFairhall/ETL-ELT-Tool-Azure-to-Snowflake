from time import sleep
from manual_config.functions import *

import manual_config.config as config
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

while True:
    try:
        # Get the private key passphrase from Azure Key Vault
        private_key_passphrase = get_private_key_passphrase(config.KEY_VAULT_URL, config.SECRET_NAME)

        # Load the private key
        private_key_text = load_private_key(config.PATH_TO_PRIVATE_KEY, private_key_passphrase)

        # Create an ingest manager
        ingest_manager = create_ingest_manager(config.ACCOUNT_IDENTIFIER, config.USER_LOGIN_NAME, config.DB_NAME, config.SCHEMA_NAME, config.PIPE_NAME, private_key_text)

        # Set up Azure Blob Storage client
        blob_service_client = setup_blob_storage_client(config.STORAGE_ACCOUNT_URL)

        # Get the blob container client
        container_client = get_blob_container_client(blob_service_client, config.CONTAINER_NAME)

        # Ingest the files
        ingest_files(ingest_manager, config.FILE_PATHS, container_client)
    except Exception as e:
        # Log the error
        logging.error(f"Failed to ingest files: {e}")
    
    # Sleep for 30 minutes
    sleep(1800)
