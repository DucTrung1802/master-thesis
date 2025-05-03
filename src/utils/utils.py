import os
import zipfile
import requests

from logger.logger import Logger
from models.tabular_database_driver_models.tabular_database_driver_models import (
    DataType,
)
from utils.enums import FileExtension


def format_value(value, data_type: DataType):
    """Format value based on its data type for SQL query."""
    match data_type:
        case DataType.VARCHAR:
            return f"'{str(value).replace("'", "''") if value else value}'"
        case DataType.DATE:
            return f"DATE '{value}'"
        case DataType.TIME:
            return f"TIME '{value}'"
        case DataType.TIMESTAMP:
            return f"TIMESTAMP '{value}'"
        case _:
            return str(value)


def remove_all_files_with_extensions(
    logger: Logger, folder_path, extensions: list[FileExtension] = None
):
    """
    Removes all files with the specified extensions from the folder.
    If the extension list is empty, deletes all files.

    Args:
        folder_path (str): The path to the folder.
        extensions (list[str]): List of file extensions to delete (e.g., [".csv", ".txt"]).
        logger (Logger): Logger instance for logging.

    Raises:
        FileNotFoundError: If the folder does not exist.
        PermissionError: If file deletion or folder access is denied.
    """
    try:
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)

            if os.path.isfile(file_path):
                _, ext = os.path.splitext(file_name)

                if not extensions or ext.lower() in [
                    e.value.lower() for e in extensions
                ]:
                    try:
                        os.remove(file_path)
                        logger.log_info(f"Deleted file: {file_path}")
                    except PermissionError as e:
                        logger.log_error(f"Permission denied: {file_path}. Error: {e}")
                    except FileNotFoundError as e:
                        logger.log_error(f"File not found: {file_path}. Error: {e}")

        logger.log_info(f"Finished deleting files in {folder_path}.")

    except FileNotFoundError as e:
        logger.log_error(f"Folder not found: {folder_path}. Error: {e}")
    except PermissionError as e:
        logger.log_error(
            f"Permission denied to access folder: {folder_path}. Error: {e}"
        )


def extract_zip_file(logger: Logger, zip_path, extract_to_folder):
    """
    Extracts the contents of a ZIP file to a specified folder.

    Args:
        zip_path (str): The path to the ZIP file to be extracted.
        extract_to_folder (str): The destination folder where the files will be extracted.
        logger (Logger): A logger instance used to log information about the extraction process.

    Returns:
        list: A list of file names that were extracted from the ZIP file.

    Raises:
        zipfile.BadZipFile: If the file is not a valid ZIP file.
        FileNotFoundError: If the specified ZIP file does not exist.
        PermissionError: If there are insufficient permissions to read the ZIP file or write to the destination folder.
    """
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        extracted_files = zip_ref.namelist()
        zip_ref.extractall(extract_to_folder)
    logger.log_info(f"Extracted files: {extracted_files}")
    return extracted_files


def rename_first_csv_file(
    logger: Logger, extracted_files, folder_path, target_file_path
):
    """
    Renames the first CSV file found in a list of extracted files to a specified target file path.

    Args:
        logger (Logger): An instance of a logger to log information and warnings.
        extracted_files (list): A list of file names extracted from a ZIP archive.
        folder_path (str): The path to the folder containing the extracted files.
        target_file_path (str): The desired file path for renaming the first CSV file.

    Behavior:
        - Iterates through the list of extracted files.
        - If a file with a ".csv" extension is found, renames it to the specified target file path.
        - Logs an informational message upon successful renaming.
        - If no CSV file is found, logs a warning message.

    Returns:
        None
    """
    for extracted_file in extracted_files:
        if extracted_file.endswith(".csv"):
            original_path = os.path.join(folder_path, extracted_file)
            os.rename(original_path, target_file_path)
            logger.log_info(f"Renamed extracted file to: {target_file_path}")
            return
    logger.log_warning("No CSV file found in ZIP archive.")


def download_file(download_url, file_path, logger):
    """
    Downloads a ZIP file from a given URL and saves it to the specified path.

    Args:
        zip_path (str): The file path where the downloaded ZIP file will be saved.
        file_url (str): The URL of the ZIP file to download.
        logger (object): A logger instance with methods `log_info` and `log_error`
                         for logging informational and error messages.

    Returns:
        None

    Logs:
        - Logs an informational message if the file is downloaded successfully.
        - Logs an error message if the download fails or an exception occurs.
    """
    try:
        response = requests.get(download_url)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
            logger.log_info(f"ZIP file downloaded to: {file_path}")
        else:
            logger.log_error(
                f"Failed to download file. Status code: {response.status_code}"
            )
            return
    except Exception as e:
        logger.log_error(f"Exception occurred while downloading ZIP file: {str(e)}")
        return
