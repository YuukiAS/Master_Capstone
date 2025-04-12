"""
SQLite database utility functions with CSV integration.

This module provides functions for managing SQLite database operations, particularly
focused on creating and updating tables using CSV data. Key functionalities include:
- Creating new tables from CSV files
- Updating existing tables with CSV data
- Querying table information and contents
- Managing column definitions and data types

Note:
    This module handles the integration between CSV files and SQLite database,
    providing functions to efficiently transfer data from CSV to database tables.

Dependencies:
    - sqlite3: For database operations
    - pandas: For data manipulation
    - pickle: For storing column definitions
"""

import pickle
import sqlite3
from tqdm import tqdm
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
from .constants import DatabaseConfig, TableNames


def print_table_info(table_name, db_file_path=DatabaseConfig.DB_PATH):
    """
    Print the information of a table in SQLite database.

    Args:
        table_name (str): Name of the table to query
        db_file_path (str, optional): Path to the SQLite database file. Defaults to DB_FILE_PATH.

    Returns:
        None: Prints the table information including column names, types, and constraints
    """
    print(f"Printing Information for table: {table_name}")
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    for column in columns:
        print(column)


def query_eids(cursor=None, db_file_path=DatabaseConfig.DB_PATH, primary_key="eid"):
    """
    Query all primary key values from the processed table.

    This function retrieves all subject IDs from the PROCESSED table in the database.
    These IDs represent the subjects that have been successfully processed and are
    available for analysis.

    Args:
        cursor (sqlite3.Cursor, optional): SQLite cursor object. If None, a new
            connection will be created. Defaults to None.
        db_file_path (str, optional): Path to the SQLite database file.
            Defaults to DatabaseConfig.DB_PATH.
        primary_key (str, optional): Name of the primary key column.
            Defaults to "eid".

    Returns:
        list[int]: List of primary key values from PROCESSED table, sorted in
            ascending order.

    Raises:
        sqlite3.Error: If database connection or query fails
    """
    if cursor is None:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
    query_eid_sql = f"SELECT {primary_key} FROM {TableNames.PROCESSED};"
    cursor.execute(query_eid_sql)
    eids = cursor.fetchall()
    eids = [eid[0] for eid in eids]
    return eids


def _get_column_defs(
    selected_column_IDs,
    primary_key="eid",
    existing_column_sql_defs=None,
    column_defs_path=DatabaseConfig.COLUMN_DEFS_PATH,
):
    """
    Get SQL column definitions based on selected column IDs.

    Args:
        selected_columns_ID (list): List of column IDs to process
        primary_key (str, optional): Name of the primary key column. Defaults to "eid"
        existing_column_sql_defs (list, optional): List of existing column definitions. Defaults to None
        column_defs_path (str, optional): Path to column definitions file. Defaults to DatabaseConfig.COLUMN_DEFS_PATH

    Returns:
        tuple: (selected_column_sql_defs, selected_column_csv)
            - selected_column_sql_defs (list): SQL definitions for selected columns
            - selected_column_csv (list): Column names for CSV extraction

    Raises:
        ValueError: If column definitions file cannot be loaded
    """

    with open(column_defs_path, "rb") as f:
        column_defs = pickle.load(f)

        # escape the numbers as SQL column names
        escaped_column_defs = [
            f"`{col_def.split()[0]}` {' '.join(col_def.split()[1:])}" if primary_key not in col_def else col_def
            for col_def in column_defs
        ]

        # select the columns based on the selected_columns_ID
        selected_column_sql_defs = []  # define Used to construct the SQL table
        for col in escaped_column_defs:
            if primary_key in col:
                continue
            col_int = col.replace("`", "")
            if int(col_int.split("-")[0]) in selected_column_IDs:
                selected_column_sql_defs.append(col)

        if existing_column_sql_defs:
            len_before = len(selected_column_sql_defs)
            selected_column_sql_defs = [
                col_def for col_def in selected_column_sql_defs if col_def not in existing_column_sql_defs
            ]
            len_after = len(selected_column_sql_defs)
            if len_before - len_after > 0:
                print(f"{len_before - len_after} columns already exist in the table")

        selected_column_csv = [primary_key]  # define Used to extract from the CSV file
        for selected_column_sql_def in selected_column_sql_defs:
            if primary_key in selected_column_sql_def:
                continue
            selected_column_sql_def = selected_column_sql_def.replace("`", "")
            selected_column_csv.append(selected_column_sql_def.split(" ")[0])

        return selected_column_sql_defs, selected_column_csv


def _process_chunk(chunk_raw, selected_column_csv, eids, primary_key="eid"):
    """
    Process a chunk of CSV data.

    Args:
        chunk_raw (pd.DataFrame): Data chunk to process
        selected_column_csv (list): List of columns to process
        eids (list): List of valid eids to filter by
        primary_key (str, optional): Name of the primary key column. Defaults to "eid"

    Returns:
        pd.DataFrame: Processed chunk with:
            - Filtered rows based on eids
            - Converted data types
            - NULL values replaced with None

    Raises:
        ValueError: If primary key contains NA values or invalid data
    """
    # Create a copy to avoid SettingWithCopyWarning
    chunk = chunk_raw.copy()

    # Convert primary key to numeric and filter out useless columns
    chunk.loc[:, primary_key] = pd.to_numeric(chunk[primary_key], errors="coerce")
    chunk = chunk[chunk[primary_key].isin(eids)]

    # Convert all selected columns to the correct type
    for col in selected_column_csv:
        if col == primary_key:
            if chunk[col].isna().any():
                raise ValueError(f"Primary key column '{col}' contains NA values")
            chunk.loc[:, col] = chunk[col].astype(int)
        else:
            try:
                chunk.loc[:, col] = pd.to_numeric(chunk[col], errors="raise")
                if is_numeric_dtype(chunk[col]):
                    if chunk[col].notna().all() and (chunk[col] % 1 == 0).all():
                        chunk.loc[:, col] = chunk[col].astype(int)
                    else:
                        chunk.loc[:, col] = chunk[col].astype(float)
            except ValueError:
                chunk.loc[:, col] = chunk[col].astype(str)

    # Replace NA values with None
    chunk = chunk.where(pd.notna(chunk), None)
    return chunk


def _get_non_empty_columns(cursor, table_name_temp, primary_key="eid"):
    """
    Get list of non-empty columns from temporary table.

    Args:
        cursor: SQLite cursor
        table_name_temp (str): Name of temporary table
        primary_key (str): Name of the primary key column. Defaults to "eid"

    Returns:
        list: List of non-empty column names
    """
    cursor.execute(f"PRAGMA table_info({table_name_temp})")
    selected_columns_sql_name = [row[1] for row in cursor.fetchall()]
    non_empty_columns_sql_name = []

    pattern_empty = None  # define the pattern of a previous empty column
    for col_name in tqdm(selected_columns_sql_name, desc="Dropping empty columns"):
        pattern_current = col_name.replace("`", "").split("-")[0]
        if pattern_empty is not None and pattern_current == pattern_empty:
            print(f"Empty column {col_name} will be dropped")
            continue
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM {table_name_temp} 
            WHERE `{col_name}` IS NOT NULL;
        """)
        count = cursor.fetchone()[0]
        if (count > 0 and col_name != primary_key) or col_name == primary_key:
            non_empty_columns_sql_name.append(col_name)
            pattern_empty = None
        else:
            print(f"Empty column {col_name} will be dropped")
            pattern_empty = col_name.replace("`", "").split("-")[0]

    return non_empty_columns_sql_name


def generate_table_from_main_csv(
    table_name,
    selected_columns_ID,
    csv_file_path=DatabaseConfig.CSV_PATH,
    db_file_path=DatabaseConfig.DB_PATH,
    chunk_size=5000,
    primary_key="eid",
):
    """
    Create a new table from the main CSV file, which contains all subject data.
    This function is specifically designed for the main CSV file of UK Biobank.

    Args:
        table_name (str): Name of the table to create
        selected_columns_ID (list): List of column IDs to extract from the CSV file
        csv_file_path (str, optional): Path to the CSV file. Defaults to DatabaseConfig.CSV_PATH
        db_file_path (str, optional): Path to the SQLite database. Defaults to DatabaseConfig.DB_PATH
        chunk_size (int, optional): Number of rows to process at once. Defaults to 5000
        primary_key (str, optional): Name of the primary key column. Defaults to "eid"

    Process:
        1. Creates a temporary table with all selected columns
        2. Loads data from CSV in chunks
        3. Removes empty columns
        4. Creates final table with non-empty columns
        5. Transfers data from temporary to final table

    Notes:
        - Only processes rows that exist in PROCESSED table
        - Automatically determines appropriate data types for columns
        - Maintains foreign key relationship with PROCESSED table
        - Skips completely empty columns

    Raises:
        ValueError: If selected_columns_ID is not a list
        ValueError: If primary key contains NA values
        sqlite3.Error: If database operations fail
    """

    # make sure selected_columns_ID is a list
    if not isinstance(selected_columns_ID, list):
        raise ValueError("selected_columns_ID must be a list")

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    table_name_temp = f"{table_name}_temp"  # define temporary table that will be dropped later

    # * Step1/5: Select based on provided selected_columns_ID
    selected_column_sql_defs, selected_column_csv = _get_column_defs(selected_columns_ID, primary_key)

    # * Step2/5: Create the temporary table
    print(f"Creating temporary table: {table_name_temp}")
    cursor.execute("PRAGMA foreign_keys = ON;")
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name_temp} (
            {primary_key} INTEGER PRIMARY KEY, 
            {", ".join(selected_column_sql_defs)},
            FOREIGN KEY ({primary_key}) REFERENCES {TableNames.PROCESSED} ({primary_key})
        );
    """
    cursor.execute(create_table_sql)

    # * Step3/5: Insert the data into the table
    eids = query_eids(cursor)

    print(f"Inserting data into temporary table: {table_name_temp}")
    with tqdm(total=DatabaseConfig.TOTAL_ROWS, desc="Processing CSV", unit="rows") as pbar:
        for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, dtype=str, usecols=selected_column_csv):
            chunk = _process_chunk(chunk, selected_column_csv, eids)
            chunk.to_sql(table_name_temp, conn, if_exists="append", index=False)  # insert the csv into SQL database
            pbar.update(chunk_size)

    # * Step4/5: Drop all columns that are empty in the temporary table
    print(f"Dropping empty columns in the temporary table: {table_name_temp}")
    non_empty_columns_sql_name = _get_non_empty_columns(cursor, table_name_temp)
    print(f"There will be {len(non_empty_columns_sql_name)} columns in the table: {table_name}")

    # * Step5/5: Create the formal table and drop the temporary table
    print(f"Creating the formal table: {table_name}")
    create_table_sql = f"""
        CREATE TABLE {table_name} (
            {primary_key} INTEGER PRIMARY KEY,
            {", ".join(f"`{col}`" for col in non_empty_columns_sql_name if col != primary_key)},
            FOREIGN KEY ({primary_key}) REFERENCES {TableNames.PROCESSED} ({primary_key})
        );
    """
    cursor.execute(create_table_sql)

    print(f"Inserting data into the formal table: {table_name}")
    cols_str = ", ".join(f"`{col}`" for col in non_empty_columns_sql_name)
    cursor.execute(f"""
        INSERT INTO {table_name} 
        SELECT {cols_str}
        FROM {table_name_temp};
    """)

    cursor.execute(f"DROP TABLE {table_name_temp}")
    conn.commit()
    conn.close()
    print(f"Table {table_name} has been created successfully.")


def update_table_from_csv(
    table_name,
    selected_columns_ID,
    csv_file_path=DatabaseConfig.CSV_PATH,
    db_file_path=DatabaseConfig.DB_PATH,
    chunk_size=5000,
    primary_key="eid",
):
    """
    Insert new columns into an existing table in SQLite database from a CSV file.

    Args:
        table_name (str): Name of the existing table to update
        selected_columns_ID (list): List of column IDs to add from the CSV file
        csv_file_path (str, optional): Path to the CSV file. Defaults to CSV_FILE_PATH.
        db_file_path (str, optional): Path to the SQLite database. Defaults to DB_FILE_PATH.
        chunk_size (int, optional): Number of rows to process at once. Defaults to 5000
        primary_key (str, optional): Name of the primary key column. Defaults to "eid"

    Process:
        1. Checks if table exists
        2. Creates temporary table for new columns
        3. Loads and processes data from CSV
        4. Removes empty columns
        5. Adds non-empty columns to existing table
        6. Updates data in the main table

    Notes:
        - Only adds columns that don't already exist in the table
        - Only processes rows that have ECG data
        - Maintains foreign key relationship with PROCESSED_TABLE_NAME
        - Skips empty columns

    Raises:
        ValueError: If table doesn't exist, if selected_columns_ID is not a list,
                   or if primary key contains NA values
    """

    # make sure selected_columns_ID is a list
    if not isinstance(selected_columns_ID, list):
        raise ValueError("selected_columns_ID must be a list")

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    table_name_temp = f"{table_name}_temp"

    # * Step1/6: Check if the table exists
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    if cursor.fetchone() is None:
        raise ValueError(f"Table {table_name} does not exist.")

    # * Step2/6: Select based on provided selected_columns_ID
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [row[1] for row in cursor.fetchall()]
    existing_column_sql_defs = [f"`{col}` {col.split()[1]}" for col in existing_columns]
    selected_column_sql_defs, selected_column_csv = _get_column_defs(selected_columns_ID, primary_key, existing_column_sql_defs)

    # * Step3/6: Create a temporary table
    print(f"Creating temporary table: {table_name_temp}")
    cursor.execute("PRAGMA foreign_keys = ON;")
    create_table_sql = f"""
        CREATE TABLE {table_name_temp} (
            {primary_key} INTEGER PRIMARY KEY,
            {", ".join(selected_column_sql_defs)},
            FOREIGN KEY ({primary_key}) REFERENCES {TableNames.PROCESSED} ({primary_key})
        );
    """
    cursor.execute(create_table_sql)

    # * Step4/6: Insert the data into the temporary table
    eids = query_eids(cursor)

    print(f"Inserting data into temporary table: {table_name_temp}")
    with tqdm(total=DatabaseConfig.TOTAL_ROWS, desc="Processing CSV", unit="rows") as pbar:
        for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, dtype=str, usecols=selected_column_csv):
            chunk = _process_chunk(chunk, selected_column_csv, eids)
            chunk.to_sql(table_name_temp, conn, if_exists="append", index=False)
            pbar.update(chunk_size)

    # * Step5/6: Drop all columns that are empty in the temporary table
    print(f"Dropping empty columns in the temporary table: {table_name_temp}")
    non_empty_columns_sql_name = _get_non_empty_columns(cursor, table_name_temp)
    print(f"There will be {len(non_empty_columns_sql_name)} columns added to the table: {table_name}")

    # * Step6/6: Add remanings columns to the formal table and drop the temporary table
    print(f"Updating the formal table: {table_name}")
    # Obtain the definitions for non-empty columns
    non_empty_columns_sql_defs = []
    for selected_column_sql_def in selected_column_sql_defs:
        if selected_column_sql_def.split(" ")[0].replace("`", "") in non_empty_columns_sql_name:
            non_empty_columns_sql_defs.append(selected_column_sql_def)
    if len(non_empty_columns_sql_defs) == 0:
        print(f"No new columns to add to the table: {table_name}")
        return

    print(f"Inserting data into the formal table: {table_name}")
    for col_def in non_empty_columns_sql_defs:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_def};")
    alter_table_sql = f"""
        UPDATE {table_name} AS t1
        SET {
        ", ".join(
            f"`{col}` = (SELECT `{col}` FROM {table_name_temp} t2 WHERE t2.{primary_key} = t1.{primary_key})"
            for col in non_empty_columns_sql_name
        )
    }        
        -- Only update rows that exist in the temporary table
        WHERE EXISTS (
            SELECT 1 
            FROM {table_name_temp} t2 
            WHERE t2.{primary_key} = t1.{primary_key}
        )
    """
    cursor.execute(alter_table_sql)

    cursor.execute(f"DROP TABLE {table_name_temp}")
    conn.commit()
    conn.close()
    print(f"Table {table_name} has been updated successfully.")


def _pandas_to_sql_type(dtype):
    """
    Convert pandas dtype to SQLite type.

    Args:
        dtype: pandas dtype object

    Returns:
        str: Corresponding SQLite type
    """
    dtype_str = str(dtype)
    if "int" in dtype_str:
        return "INTEGER"
    elif "float" in dtype_str:
        return "REAL"
    elif "bool" in dtype_str:
        return "INTEGER"  # SQLite doesn't have boolean type
    else:
        return "TEXT"


def generate_table_from_result_csv(
    table_name,
    csv_file_path,
    column_names: list[str] = None,
    db_file_path=DatabaseConfig.DB_PATH,
    primary_key="eid",
    drop_NA=True,
    drop_existing=True,
    precision=6,
):
    """
    Create a new table from a result CSV file containing analysis outputs.
    This function is designed for smaller CSV files generated as analysis results.

    Args:
        table_name (str): Name of the table to create
        csv_file_path (str): Path to the result CSV file
        column_names (list[str], optional): List of columns to include in table.
            If None, all columns are included. Defaults to None
        db_file_path (str, optional): Path to the SQLite database.
            Defaults to DatabaseConfig.DB_PATH
        primary_key (str, optional): Name of the primary key column.
            Defaults to "eid"
        drop_NA (bool, optional): Whether to drop columns with all NA values.
            Defaults to True
        drop_existing (bool, optional): Whether to drop the table if it exists.
            Defaults to True
        precision (int, optional): Number of decimal places for float values.
            Defaults to 6

    Process:
        1. Checks and drops existing table if requested
        2. Reads the CSV file into memory
        3. Sets precision for float columns
        4. Filters columns if column_names provided
        5. Drops columns with all NA values if drop_NA is True
        6. Creates table with foreign key constraint to PROCESSED table
        7. Inserts data from DataFrame

    Notes:
        - Designed for smaller result files, not the main CSV
        - Loads entire file into memory (not chunked)
        - Automatically determines column types
        - Maintains foreign key relationship with PROCESSED table
        - Float values are formatted with specified precision
        - Drops columns (not rows) with all NA values
        - Creates table manually to ensure foreign key constraint

    Raises:
        ValueError: If primary key column is missing
        sqlite3.Error: If database operations fail
        FileNotFoundError: If CSV file doesn't exist
        pd.errors.EmptyDataError: If CSV file is empty
        TypeError: If column_names is provided but not a list
    """
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    if drop_existing:
        table_existing = cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';").fetchone()
        if table_existing:
            print(f"Table {table_name} exists and will be dropped")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    df = pd.read_csv(csv_file_path)
    # set precision for all float columns
    float_cols = df.select_dtypes(include=["float64", "float32"]).columns
    for col in float_cols:
        df[col] = df[col].apply(
            lambda x: np.format_float_positional(x, precision=precision, unique=False, trim="k") if pd.notnull(x) else x
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if column_names:
        df = df[[primary_key, *column_names]]

    if drop_NA:
        df_shape_before = df.shape
        df.dropna(how="all", inplace=True, axis=1)  # drop columns instead of rows
        df_shape_after = df.shape
        print(f"Dropped NA columns: {df_shape_before[1] - df_shape_after[1]} columns are dropped")

    df_columns = df.columns.tolist()
    df_types = df.dtypes.to_dict()

    if primary_key not in df_columns:
        raise ValueError(f"Primary key column '{primary_key}' not found in CSV file")

    column_sql_defs = []
    for col in df_columns:
        if col != primary_key:
            sql_type = _pandas_to_sql_type(df_types[col])
            column_sql_defs.append(f"{col} {sql_type}")

    print(f"Creating table: {table_name}")
    cursor.execute("PRAGMA foreign_keys = ON;")
    # * We should not create table directly from pandas dataframe, as SQLite doesn't support adding foreign key after creation
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {primary_key} INTEGER PRIMARY KEY, 
            {", ".join(column_sql_defs)},
            FOREIGN KEY ({primary_key}) REFERENCES {TableNames.PROCESSED} ({primary_key})
        );
    """
    cursor.execute(create_table_sql)

    # Insert values into the table
    df.to_sql(table_name, conn, if_exists="append", index=False)

    conn.commit()
    conn.close()
    print(f"Table {table_name} has been created successfully.")


def drop_column_from_table(table_name, column_name, db_file_path=DatabaseConfig.DB_PATH):
    """
    Remove a column from an existing table in SQLite database.

    Args:
        table_name (str): Name of the table to modify
        column_name (str): Name of the column to remove
        db_file_path (str, optional): Path to the SQLite database.
            Defaults to DatabaseConfig.DB_PATH

    Process:
        - Connects to the database
        - Drops the column from the table
        - Commits and closes the connection
    """
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    # * Step1/4: Check if the column exists
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [row[1] for row in cursor.fetchall()]
    if column_name not in existing_columns:
        raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")

    # * Step2/4: Create a new table with the column dropped
    used_columns = [col for col in existing_columns if col != column_name]
    table_name_temp = f"{table_name}_temp"
    cursor.execute(f"CREATE TABLE {table_name_temp} AS SELECT {', '.join(used_columns)} FROM {table_name}")

    # * Step3/4: Drop the original table
    cursor.execute(f"DROP TABLE {table_name}")

    # * Step4/4: Rename the new table to the original table name
    cursor.execute(f"ALTER TABLE {table_name_temp} RENAME TO {table_name}")
    conn.commit()
    conn.close()
    print(f"Column '{column_name}' has been removed from table '{table_name}'")
