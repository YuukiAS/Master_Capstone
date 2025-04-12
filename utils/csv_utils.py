"""
Utility functions for CSV file operations.

This module provides functions for reading and manipulating CSV files without any
database interactions. It includes functions for:
- Counting rows in CSV files
- Reading column names and definitions
- Previewing CSV data
- Extracting specific columns from CSV files

Note:
    All functions in this module operate directly on CSV files and do not require
    or interact with any database connections.

Dependencies:
    - pandas: For DataFrame operations
    - dask: For handling large CSV files
    - IPython: For display in Jupyter notebooks
"""

import csv
import pandas as pd
from IPython.display import display, HTML
import dask.dataframe as dd
import pickle
from tqdm import tqdm

from .constants import DatabaseConfig


def count_row_numbers(file_path=DatabaseConfig.CSV_PATH):
    """
    Count the total number of rows in a CSV file.

    Args:
        file_path (str, optional): Path to the CSV file. Defaults to FILE_PATH.

    Returns:
        int: Total number of rows in the CSV file, including header
    """
    with open(file_path, mode="r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        row_count = sum(1 for row in reader)
    return row_count


def get_column_names(file_path=DatabaseConfig.CSV_PATH):
    """
    Get the names of all columns from the first row of a CSV file.

    Args:
        file_path (str, optional): Path to the CSV file. Defaults to FILE_PATH.

    Returns:
        list: List of column names from the CSV header
    """
    with open(file_path, "r", newline="") as csvfile:
        reader = csv.reader(csvfile)
        column_names = next(reader)
    return column_names


def gen_column_names(target_column_ids, file_path=DatabaseConfig.CSV_PATH):
    """
    Generate a list of column names from a list of column IDs.

    Args:
        target_column_ids (list): List of target column IDs
        file_path (str, optional): Path to the CSV file. Defaults to DatabaseConfig.CSV_PATH

    Returns:
        list: List of corresponding column names, excluding 'eid' column
    """
    column_names = get_column_names(file_path)
    target_column_names = []
    for column_name in column_names:
        if column_name == "eid":
            continue
        column_id = int(column_name.split("-")[0])
        if column_id in target_column_ids:
            target_column_names.append(column_name)
    return target_column_names


def peek_data(
    target_column_names, file_path=DatabaseConfig.CSV_PATH, nrows=1000, drop_NA=False
):
    """
    Preview specific columns from a CSV file in a Jupyter notebook.

    Args:
        target_column_names (list): List of column names to extract from the CSV
        file_path (str, optional): Path to the CSV file. Defaults to DatabaseConfig.CSV_PATH
        nrows (int, optional): Number of rows to preview. Defaults to 1000
        drop_NA (bool, optional): Whether to drop columns with all NA values. Defaults to False

    Process:
        1. Efficiently reads specified number of rows using pandas
        2. Attempts to convert string columns to numeric where possible
        3. Removes columns containing only NA values if drop_NA is True
        4. Displays data as HTML table

    Returns:
        pandas.DataFrame: DataFrame containing the preview data
    """
    # * We specify all dtypes to be object, so that no error will be raised if some columns only have NA values.
    # * If we only want first few rows, we don't need to use dd.read_csv, but pd.read_csv instead
    df = pd.read_csv(
        file_path,
        usecols=["eid", *target_column_names],
        header=0,
        dtype={"eid": "int64", **{col: "object" for col in target_column_names}},
        nrows=nrows,
    )

    # After reading the data, convert columns to numeric if possible
    for col in df.columns:
        if col != "eid" and df[col].dtype == "string":
            try:
                converted = pd.to_numeric(df[col])
                df[col] = converted
            except ValueError:
                pass

    # Drop those columns with all NA values, even if the column is in the eids list
    if drop_NA:
        cols_to_drop = [
            col for col in df.columns if col != "eid" and df[col].isna().all()
        ]
        df = df.drop(columns=cols_to_drop)

    display(HTML(df.to_html()))

    return df


def get_data(
    target_column_names, file_path=DatabaseConfig.CSV_PATH, eids=None, drop_NA=False
):
    """
    Read specific columns from a CSV file into a pandas DataFrame.

    Args:
        target_column_names (list): List of column names to extract from the CSV
        file_path (str, optional): Path to the CSV file. Defaults to DatabaseConfig.CSV_PATH
        eids (list, optional): List of specific eids to filter the data. Defaults to None
        drop_NA (bool, optional): Whether to drop columns with all NA values. Defaults to False

    Process:
        1. Reads data efficiently using dask for large datasets
        2. Filters rows by eids if provided
        3. Converts string columns to numeric where possible
        4. Removes columns containing only NA values if drop_NA is True

    Returns:
        pandas.DataFrame: DataFrame containing requested columns and filtered rows

    Raises:
        ValueError: If eids is provided but is not a list
    """

    df = dd.read_csv(
        file_path,
        usecols=["eid", *target_column_names],
        header=0,
        sample=1000000,  # Uses 1MB for each block during sampling to determine dtypes
        sample_rows=100,  # Number of rows that need to read in each block
        dtype={"eid": "int64", **{col: "object" for col in target_column_names}},
    ).compute()

    if eids is not None:
        if not isinstance(eids, list):
            raise ValueError("eids must be a list")
        df = df[df["eid"].isin(eids)]

    for col in tqdm(df.columns, desc="Converting columns to numeric if possible"):
        if col != "eid" and df[col].dtype == "string":
            try:
                converted = pd.to_numeric(df[col])
                df[col] = converted
            except ValueError:
                pass

    # Drop all rows with all NA values, even if the row is in the eids list
    if drop_NA:
        cols_to_drop = [
            col for col in df.columns if col != "eid" and df[col].isna().all()
        ]
        df = df.drop(columns=cols_to_drop)
    return df


def generate_column_defs(file_path=DatabaseConfig.CSV_PATH):
    """
    Generate and save definitions for all columns in a CSV file to a pickle file.

    Args:
        file_path (str, optional): Path to the CSV file. Defaults to DatabaseConfig.CSV_PATH

    Process:
        1. Gets all column names
        2. Reads data sample to determine column data types
        3. Generates column definitions (name and dtype)
        4. Saves definitions to pickle a file

    Returns:
        None: Saves column definitions to file specified in DatabaseConfig.COLUMN_DEFS_PATH
    """
    column_names = get_column_names(file_path)
    print("Detecting data types of all columns")
    df = peek_data(column_names, file_path)
    # get the definitions of all columns
    print("Generating column definitions")
    column_defs = []
    for col in df.columns:
        column_defs.append(f"{col} {df[col].dtype}")

    with open(DatabaseConfig.COLUMN_DEFS_PATH, "wb") as file:
        pickle.dump(column_defs, file)

    print(f"Column definitions have been saved to {DatabaseConfig.COLUMN_DEFS_PATH}.")
