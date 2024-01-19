# Snowflake Data Ingestion Function

This repo contains Python scripts for ingesting data from Azure Blob Storage into Snowflake. 

It includes two versions: 

- A **manual version** utilising a adjustable `config.py` for testing purposes.
  
- A **HTTP request version** for use with Azure Functions and other tools such as Azure Data Factory.

## Manual Version

The manual version uses environment variables for configuration and runs in an infinite loop, ingesting files every 30 minutes.

### Key Files

- `config.py`: This file sets up various environment variables for the application.
  
- `functions.py`: This file contains several functions that interact with Azure Key Vault, Snowflake, and Azure Blob Storage.
  
- `main.py`: This script is the main driver of the application. It uses the functions defined in `functions.py` and the configurations set in `config.py`.

## HTTP Request Version

The HTTP request version is designed to be used as an Azure Function. It accepts parameters from an HttpRequest, allowing it to be called from an Azure Data Factory pipeline, running in an infinite loop, ingesting files every 30 minutes.

### Key Files

- `http_config/functions.py`: This file contains the same functions as `functions.py`, but designed to work with parameters from an HttpRequest.
  
- `http_config/main.py`: This script is the main driver of the application when used as an Azure Function. It uses the functions defined in `http_config/functions.py`.

## Usage

For the manual version, set the necessary environment variables and run `main.py`. For the HTTP request version, deploy `http_config/main.py`, ensuring you have the correct permissions to access the respective endpoints. 

For the Azure Function version, you will need to implement the `http_config/functions.py` and `http_config/main.py` within the Azure Function as well and call the function with the necessary parameters in the body of an HttpRequest provided by Azure Data Factory, `https_config/body.json` has been provided as an example of the json body that can be passed. 

## License

This project is licensed under the MIT License.