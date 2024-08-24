import azure.functions as func
from http_config.functions import (
    get_private_key_passphrase,
    load_private_key,
    create_ingest_manager,
    setup_blob_storage_client,
    get_blob_container_client,
    ingest_files,
    logging,
)
import time


def main(req: func.HttpRequest) -> func.HttpResponse:
    # Parse parameters from the HttpRequest body
    try:
        params = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    key_vault_url = params.get("key_vault_url")
    secret_name = params.get("secret_name")
    account_identifier = params.get("account_identifier")
    user_login_name = params.get("user_login_name")
    db_name = params.get("db_name")
    schema_name = params.get("schema_name")
    pipe_name = params.get("pipe_name")
    path_to_private_key = params.get("path_to_private_key")
    storage_account_url = params.get("storage_account_url")
    container_name = params.get("container_name")
    file_paths = params.get("file_paths").split(",")

    try:
        # Get the private key passphrase from Azure Key Vault
        private_key_passphrase = get_private_key_passphrase(key_vault_url, secret_name)

        # Load the private key
        private_key_text = load_private_key(path_to_private_key, private_key_passphrase)

        # Create an ingest manager
        ingest_manager = create_ingest_manager(
            account_identifier,
            user_login_name,
            db_name,
            schema_name,
            pipe_name,
            private_key_text,
        )

        # Set up Azure Blob Storage client
        blob_service_client = setup_blob_storage_client(storage_account_url)

        # Get the blob container client
        container_client = get_blob_container_client(
            blob_service_client, container_name
        )

        # Ingest the files
        ingest_files(ingest_manager, file_paths, container_client)

        # Sleep for 30 minutes
        time.sleep(1800)

        return func.HttpResponse("Files ingested successfully", status_code=200)
    except Exception as e:
        # Log the error
        logging.error(f"Failed to ingest files: {e}")
        return func.HttpResponse(f"Failed to ingest files: {e}", status_code=500)
