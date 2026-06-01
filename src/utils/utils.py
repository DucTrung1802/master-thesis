from dataclasses import asdict
from datetime import datetime, timedelta
import json
import os
import shutil
import zipfile
import numpy as np
import requests
from typing import Tuple, List, Optional, Union
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import time
from pathlib import Path

from dtos.model_dtos.model_output_dto import ModelOutputDto
from logger.logger import Logger
from dtos.tabular_database_driver_dtos.tabular_database_driver_dtos import (
    DataType,
)
from utils.enums import *


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
    logger: Logger, folder_path, extensions: List[FileExtension] = None
):
    """
    Removes all files with the specified extensions from the folder.
    If the extension list is empty, deletes all files.

    Args:
        folder_path (str): The path to the folder.
        extensions (List[str]): List of file extensions to delete (e.g., [".csv", ".txt"]).
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
                ext = ext.replace(".", "", 1)  # Remove leading dot from extension

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


def get_all_file_names_with_extensions(
    logger: Logger, folder_path: str, extensions: List[FileExtension] = None
) -> List[str]:
    """
    Retrieves all file names in the specified folder, optionally filtered by extensions.

    Args:
        logger (Logger): Logger instance for logging.
        folder_path (str): Path to the folder.
        extensions (List[FileExtension], optional): List of file extensions to include
            (e.g., [".csv", ".txt"]). If None, all files are returned.

    Returns:
        List[str]: List of matching file paths (with forward slashes).

    Raises:
        FileNotFoundError: If the folder does not exist.
        PermissionError: If access to the folder is denied.
    """
    matching_files = []  # List to store matching file paths

    try:
        # Iterate through all files and folders in the given directory
        for file_name in os.listdir(folder_path):
            # Construct the full file path using pathlib for cross-platform compatibility
            file_path = Path(folder_path) / file_name

            # Check if the current path is a file (not a directory)
            if file_path.is_file():
                # Extract the file extension without the leading dot (e.g., ".csv" -> "csv")
                ext = file_path.suffix.lstrip(".")

                # Check if extensions filter is provided and match the file extension
                # If no filter is provided, include all files
                if not extensions or ext.lower() in [
                    e.value.lower() for e in extensions
                ]:
                    # Convert the path to a POSIX-style string (uses forward slashes)
                    matching_files.append(file_path.as_posix())

        # Log how many matching files were found in the folder
        logger.log_info(f"Found {len(matching_files)} matching files in {folder_path}.")
        return matching_files

    except FileNotFoundError as e:
        # Log and re-raise if the folder does not exist
        logger.log_error(f"Folder not found: {folder_path}. Error: {e}")
        raise
    except PermissionError as e:
        # Log and re-raise if there is a permission error
        logger.log_error(
            f"Permission denied to access folder: {folder_path}. Error: {e}"
        )
        raise


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


def format_key_for_name(key):
    return "_".join(
        k.name.lower() if isinstance(k, Enum) else str(k).lower() for k in key
    )


def format_key_for_path(key):
    return "/".join(
        k.name.lower() if isinstance(k, Enum) else str(k).lower() for k in key
    )


def format_key_for_table(key: Tuple):
    return ".".join(k.name.lower() for k in key)


def get_newest_file_path(folder_path, extension: FileExtension = None):
    """
    Returns the path of the newest (most recently modified) file in the given folder,
    optionally filtered by a single file extension.

    Parameters:
        folder_path (str): The path to the folder to search.
        extension (FileExtension, optional): File extension to filter by (e.g., '.txt').
                                   If None, all files are considered.

    Returns:
        str or None: Path to the newest file, or None if no matching file is found.
    """
    if not os.path.isdir(folder_path):
        raise ValueError(f"'{folder_path}' is not a valid directory.")

    newest_file = None
    newest_mtime = -1

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        if not os.path.isfile(file_path):
            continue

        if extension and not filename.lower().endswith(extension.value.lower()):
            continue

        mtime = os.path.getmtime(file_path)
        if mtime > newest_mtime:
            newest_mtime = mtime
            newest_file = file_path

    return newest_file


def folder_contains_files(
    folder_path: str, extension: Optional[FileExtension] = None
) -> bool:
    if not os.path.isdir(folder_path):
        return False

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            if extension is None:
                return True
            if filename.lower().endswith(f".{extension.value.lower()}"):
                return True

    return False


# Volume: remove 'K', 'M' 'B' and convert to float with multiplier
def parse_volume(val):
    if pd.isna(val):
        return None
    val = str(val).strip().replace(",", "")
    if val.endswith("K"):
        return float(val[:-1]) * 1_000
    elif val.endswith("M"):
        return float(val[:-1]) * 1_000_000
    elif val.endswith("B"):
        return float(val[:-1]) * 1_000_000_000
    else:
        return float(val)


def divided_into_chunks(lst: List, x: int) -> List[List]:
    """
    Divides a list into n chunks.

    Args:
        lst (list): The list to be divided.
        n (int): The number of chunks.

    Returns:
        list: A list containing n chunks of the original list.
    """

    if not isinstance(lst, list):
        try:
            lst = list(lst)
        except TypeError:
            return []

    n = len(lst)
    if n == 0 or x <= 0:
        return []
    bin_size = n // x
    remainder = n % x
    chunks = []
    start = 0

    for i in range(x):
        end = start + bin_size + (1 if i < remainder else 0)
        chunks.append(lst[start:end])
        start = end
    return chunks


def make_date_time_index_for_dataframe(
    df: pd.DataFrame,
    start_date: datetime = SCRAPER_START_DATE,
    end_date: datetime = SCRAPER_END_DATE,
) -> pd.DataFrame:
    generate_date_time_type = None

    # Detect type based on dataframe columns
    if {"year", "month", "day"}.issubset(df.columns):
        generate_date_time_type = GenerateDateTimeType.DAY
    elif {"year", "quarter"}.issubset(df.columns):
        generate_date_time_type = GenerateDateTimeType.QUARTER
    elif {"year", "month"}.issubset(df.columns):
        generate_date_time_type = GenerateDateTimeType.MONTH
    elif (
        "year" in df.columns
        and len(df.columns.intersection({"quarter", "month", "date"})) == 0
    ):
        generate_date_time_type = GenerateDateTimeType.YEAR
    elif "date" in df.columns:
        generate_date_time_type = GenerateDateTimeType.DATE
    else:
        raise ValueError("DataFrame does not contain recognizable time columns")

    # Build "date" column from existing fields
    match generate_date_time_type:
        case GenerateDateTimeType.YEAR:
            df["date"] = (
                pd.to_datetime(df["year"], format="%Y") + pd.offsets.YearEnd(0)
            ).dt.normalize()
            df = df.drop(columns=["year"])

        case GenerateDateTimeType.QUARTER:
            df["date"] = (
                pd.PeriodIndex.from_fields(
                    year=df["year"], quarter=df["quarter"], freq="Q"
                )
                .to_timestamp(how="end")
                .normalize()
            )
            df = df.drop(columns=["year", "quarter"])

        case GenerateDateTimeType.MONTH:
            df["date"] = (
                pd.to_datetime(
                    df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2),
                    format="%Y-%m",
                )
                + pd.offsets.MonthEnd(0)
            ).dt.normalize()
            df = df.drop(columns=["year", "month"])

        case GenerateDateTimeType.DAY:
            # Combine year, month, day into a single datetime
            df["date"] = pd.to_datetime(
                df[["year", "month", "day"]].astype(str).agg("-".join, axis=1),
                format="%Y-%m-%d",
            ).dt.normalize()
            df = df.drop(columns=["year", "month", "day"])

        case GenerateDateTimeType.DATE:
            # Already has a date column
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d").dt.normalize()
            # nothing else to drop

        case _:
            raise ValueError("Unsupported generate_date_time_type")

    # --- NEW PART: generate full date range ---
    match generate_date_time_type:
        case GenerateDateTimeType.YEAR:
            full_range = pd.date_range(
                start=start_date, end=end_date, freq="YE"
            ).normalize()

        case GenerateDateTimeType.QUARTER:
            full_range = pd.date_range(
                start=start_date, end=end_date, freq="QE"
            ).normalize()

        case GenerateDateTimeType.MONTH:
            full_range = pd.date_range(
                start=start_date, end=end_date, freq="ME"
            ).normalize()

        case GenerateDateTimeType.DAY | GenerateDateTimeType.DATE:
            full_range = pd.date_range(
                start=start_date, end=end_date, freq="D"
            ).normalize()

        case _:
            raise ValueError("Unsupported generate_date_time_type")

    full_df = pd.DataFrame({"date": full_range})

    # Merge to ensure full coverage
    df = pd.merge(full_df, df, on="date", how="left")

    # Move "date" column to the first position (already is, but keep consistent)
    cols = ["date"] + [col for col in df.columns if col != "date"]
    df = df[cols]

    return df


def standardize_time_frame(
    df: pd.DataFrame,
    start_date: datetime = SCRAPER_START_DATE,
    end_date: datetime = SCRAPER_END_DATE,
) -> pd.DataFrame:
    # Detect type and generate date column
    if {"year", "month", "day"}.issubset(df.columns):
        # Daily data
        df["date"] = pd.to_datetime(df[["year", "month", "day"]])
    elif {"year", "quarter"}.issubset(df.columns):
        # Quarterly data: last day of quarter
        df["month"] = df["quarter"] * 3
        df["date"] = pd.to_datetime(
            df[["year", "month"]].assign(day=1)
        ) + pd.offsets.MonthEnd(0)
    elif {"year", "month"}.issubset(df.columns):
        # Monthly data: last day of month
        df["date"] = pd.to_datetime(
            df[["year", "month"]].assign(day=1)
        ) + pd.offsets.MonthEnd(0)
    elif (
        "year" in df.columns
        and len(df.columns.intersection({"quarter", "month", "date"})) == 0
    ):
        # Yearly data: last day of year
        df["date"] = pd.to_datetime(df["year"].astype(str) + "-12-31")
    elif "date" in df.columns:
        # Already a date column
        df["date"] = pd.to_datetime(df["date"])
    else:
        raise ValueError("DataFrame does not contain recognizable time columns")

    # Generate full daily date range
    full_dates = pd.date_range(start=start_date, end=end_date, freq="D")

    # Reindex to include all dates, existing data stays, missing dates = NaN
    df = (
        df.set_index("date")
        .reindex(full_dates)
        .reset_index()
        .rename(columns={"index": "date"})
    )

    return df


def remove_time_column_name(column_names: List[str]) -> List[str]:
    time_column_names = ["year", "quarter", "month", "day", "date"]
    return [name for name in column_names if name.lower() not in time_column_names]


def cast_columns_to_float(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if col.lower() != "date":
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def expand_date_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # --- Basic components ---
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["day_of_week"] = df["date"].dt.dayofweek
    df["day_of_year"] = df["date"].dt.dayofyear
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df["quarter"] = df["date"].dt.quarter
    df["month_of_quarter"] = ((df["month"] - 1) % 3) + 1

    # --- Boolean flags (convert to int) ---
    for col in [
        "is_month_start",
        "is_month_end",
        "is_quarter_start",
        "is_quarter_end",
    ]:
        df[col] = getattr(df["date"].dt, col).astype(int)

    # --- Additional features ---
    df["week_of_month"] = df["date"].apply(lambda d: int(np.ceil(d.day / 7)))
    df["half_of_year"] = np.where(df["month"] <= 6, 1, 2)

    # --- Season mapping (encoded 1–4) ---
    # 1: Winter, 2: Spring, 3: Summer, 4: Fall
    season_map = {
        12: 1,
        1: 1,
        2: 1,  # Winter
        3: 2,
        4: 2,
        5: 2,  # Spring
        6: 3,
        7: 3,
        8: 3,  # Summer
        9: 4,
        10: 4,
        11: 4,  # Fall
    }
    df["season"] = df["month"].map(season_map)

    # --- Cyclical encoding for periodic features ---
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    df["day_of_week_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["day_of_week_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    df["day_of_year_sin"] = np.sin(2 * np.pi * df["day_of_year"] / 365)
    df["day_of_year_cos"] = np.cos(2 * np.pi * df["day_of_year"] / 365)

    return df


def plot_model_result(model_output_dto: ModelOutputDto):

    plt.figure(figsize=(12, 6))
    plt.plot(
        model_output_dto.y_true,
        label="True Values",
        color="blue",
        linewidth=2,
    )
    plt.plot(
        model_output_dto.y_pred_denorm,
        label="Predicted Values",
        color="red",
        linestyle="--",
        linewidth=2,
    )
    plt.title("Model Predictions vs True Values")
    plt.xlabel("Day")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(True)
    plt.show()


def move_column_to_end(df: pd.DataFrame, output_column: str) -> pd.DataFrame:
    """
    Move a specified column to the end of a pandas DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame.
    output_column : str
        The name of the column to move to the last position.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with the specified column moved to the end.

    Raises
    ------
    KeyError
        If the specified column does not exist in the DataFrame.

    Example
    -------
    >>> import pandas as pd
    >>> df = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
    >>> move_column_to_end(df, "B")
       A  C  B
    0  1  5  3
    1  2  6  4
    """
    if output_column not in df.columns:
        raise KeyError(f"Column '{output_column}' not found in DataFrame.")

    reordered_columns = [col for col in df.columns if col != output_column] + [
        output_column
    ]
    return df[reordered_columns]


def move_file(path_a, path_b):
    """
    Move a file from path_a to path_b.

    :param path_a: Source file path
    :param path_b: Destination file path (including filename)
    """
    if not os.path.isfile(path_a):
        raise FileNotFoundError(f"Source file not found: {path_a}")

    # Create destination directory if it doesn't exist
    os.makedirs(os.path.dirname(path_b), exist_ok=True)

    shutil.move(path_a, path_b)
    print(f"File moved from '{path_a}' to '{path_b}'")


def convert_xlsx_to_csv(file_path, remove_original=True):
    """
    Convert an Excel (.xlsx) file to CSV.

    Parameters:
        file_path (str): Path to the .xlsx file
        remove_original (bool): Whether to delete the original .xlsx file (default=True)

    Returns:
        str: Path to the generated .csv file
    """
    if not file_path.lower().endswith(".xlsx"):
        raise ValueError("The provided file is not an .xlsx file")

    # Read Excel file
    df = pd.read_excel(file_path)

    # Create output CSV path
    csv_path = os.path.splitext(file_path)[0] + ".csv"

    # Save as CSV
    df.to_csv(csv_path, index=False)

    # Remove original file if requested
    if remove_original:
        os.remove(file_path)

    return csv_path


def wait_for_file(file_path, timeout=10, poll_interval=0.25):
    """
    Wait until a file exists or timeout is reached.

    Parameters:
        file_path (str): Path to the file to wait for
        timeout (float): Maximum time to wait in seconds
        poll_interval (float): How often to check (seconds), default=0.25

    Returns:
        bool: True if file exists within timeout, False otherwise
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        if os.path.exists(file_path):
            return True
        time.sleep(poll_interval)

    return False


def get_current_run_path():
    log_dir = Path("lightning_logs")
    versions = [
        d for d in log_dir.iterdir() if d.is_dir() and d.name.startswith("version_")
    ]
    latest = max(versions, key=lambda x: int(x.name.split("_")[1]))

    return latest


def save_prediction_figure(y_test, y_predict, save_path, title):
    """Save prediction vs actual plot."""
    fig, ax = plt.subplots(figsize=(15, 6))

    ax.set_title(title)
    y_test.plot(ax=plt.gca(), label="close")
    y_predict.plot(ax=plt.gca(), label="predict", marker=None)

    ax.legend()
    fig.tight_layout()
    fig.savefig(save_path, dpi=300)
    plt.close(fig)


def get_weekends(from_date: str, to_date: str):
    start = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")

    weekends = []
    current = start

    while current <= end:
        if current.weekday() in (5, 6):  # 5 = Saturday, 6 = Sunday
            weekends.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return weekends


def first_day_of_month(dt: datetime) -> datetime:
    return dt.replace(day=1)


def month_ranges(
    from_date: datetime, to_date: datetime
) -> List[Tuple[datetime, datetime]]:
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")

    ranges: List[Tuple[datetime, datetime]] = []

    # start at the first day of the starting month
    current = from_date.replace(day=1)

    while current <= to_date:
        # first day of this month
        first_day = current

        # move to next month
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)

        # last day is one day before next month
        last_day = next_month - timedelta(days=1)

        ranges.append((first_day, last_day))

        current = next_month

    return ranges


def get_value(x):
    return x.value if isinstance(x, Enum) else x


def is_weekend(date_input: Union[datetime, str]) -> bool:
    """
    Check whether a given date is a weekend (Saturday or Sunday).

    Args:
        date_input: A datetime object or an ISO format string (YYYY-MM-DD).

    Returns:
        True if the date is Saturday or Sunday, otherwise False.
    """
    # Convert string input to datetime if needed
    if isinstance(date_input, str):
        date_input = datetime.fromisoformat(date_input)

    # weekday(): Monday=0 ... Sunday=6
    return date_input.weekday() >= 5
