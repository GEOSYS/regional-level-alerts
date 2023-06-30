import logging
from enum import Enum
import os
import pandas
import tempfile
from datetime import datetime
import boto3
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class NumbersComparisonOperator(str, Enum):
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN_OR_EQUAL = "<="


def get_enum_member_from_name(enum_class, name):
    for member in enum_class.__members__.values():
        if member.name == name:
            return member
    raise ValueError(f"No matching enum member found for name {name}")


def save_dataframe_to_temporary_csv(df: pandas.DataFrame, optional_filename_suffix = ""):
    """
        Save a pandas.DataFrame into a csv in a temporay folder.
        Output Filename : "Year-Month-Day_Hour-Minute-Second_regional-level-alerts"

        Args:
            - df: the DataFrame to save
            - optional_filename_suffix: a suffix to add into filename (Ex : "_weather" or "_vegetation"), default empty

        Returns:
            The complete csv file path
    """
    # Make a valid path whatever the OS
    prefix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_regional-level-alerts")
    suffix = ".csv"
    file_path_and_name = os.path.join(tempfile.gettempdir(), prefix + optional_filename_suffix + suffix)
    logging.info("RegionalLevelAlerts:save_dataframe_to_temporary_csv: Saving to csv file: " + file_path_and_name)

    # save dataframe and return complete csv file path
    df.to_csv(file_path_and_name, index=False)
    return file_path_and_name


def write_file_to_aws_s3(local_file_path):
    """
        Upload file to AWS S3 Bucket. Use .env file to get access informations.

        Args:
            - local_file_path to upload

        Returns:
            True or False depending on the success of the upload
    """
    # AWS s3 access informations
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('AWS_BUCKET_NAME')

    if access_key is not None and secret_key is not None and bucket_name is not None and access_key != "" and secret_key != "" and bucket_name != "":

        try:
            file_name = os.path.basename(local_file_path)

            # Declare s3 resource and upload file
            s3 = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            s3.Object(bucket_name, file_name).delete()
            s3.Object(bucket_name, file_name).upload_file(local_file_path)
    
            return True

        except Exception as exc:
            logging.error(f"Error while uploading file to AWS S3: {str(exc)}")
            return False

    else:
        logging.error("Please enter valid access informations to AWS S3 in .env file")
        return False


def write_file_to_azure_blob_storage(local_file_path):
    """
        Upload file to Azure Blob Storage. Use .env file to get access informations.

        Args:
            - local_file_path to upload

        Returns:
            True or False depending on the success of the upload
    """
    # Blob storage access informations
    account_name = os.getenv('AZURE_ACCOUNT_NAME')
    blob_container_name = os.getenv('AZURE_BLOB_CONTAINER_NAME')
    sas_credential = os.getenv('AZURE_SAS_CREDENTIAL')

    if account_name is not None and blob_container_name is not None and sas_credential is not None and account_name != "" and blob_container_name != "" and sas_credential != "":
        
        try:
            blob_name = os.path.basename(local_file_path)
            account_url = f"https://{account_name}.blob.core.windows.net"

            # Upload file
            blob_service_client = BlobServiceClient(account_url = account_url, credential = sas_credential)
            blob_client = blob_service_client.get_blob_client(blob_container_name, blob_name)
            with open(local_file_path, "rb") as local_file:
                blob_client.upload_blob(local_file)
        
            return True

        except Exception as exc:
            logging.error(f"Error while uploading file to Azure: {str(exc)}")
            return False

    else:
        logging.error("Please enter valid access informations to Azure blob storage in .env file")
        return False
