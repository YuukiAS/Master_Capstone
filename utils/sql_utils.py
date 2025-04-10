import pickle
import sqlite3
from tqdm import tqdm
import pandas as pd
from pandas.api.types import is_numeric_dtype

CSV_FILE_PATH = "/work/users/y/u/yuukias/database/UKBiobank/ukb673099_Jun2023.csv"
DB_FILE_PATH = "/work/users/y/u/yuukias/BIOS-Material/BIOS992/data/ukbiobank.db" 
COLUMN_DEFS_PATH = "/work/users/y/u/yuukias/BIOS-Material/BIOS992/data/column_defs.pkl"

PROCESSED_TABLE_NAME = "Processed"  # This is the main table, and we will use its eid as the foreign key for the variables table
TOTAL_ROWS = 502368
USED_ROWS = 77888
PRIMARY_KEY = "eid"


def print_table_info(table_name, db_file_path=DB_FILE_PATH):
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    for column in columns:
        print(column)


def generate_table_from_csv(
    table_name,
    selected_columns_ID,
    csv_file_path=CSV_FILE_PATH,
    db_file_path=DB_FILE_PATH,
    chunk_size=5000,
    primary_key=PRIMARY_KEY,
):
    """
    Extract the data from the csv file and insert it into the table based on the selected_columns_ID.
    Only those with ECG data will be inserted. A new table will be created.
    """

    # make sure selected_columns_ID is a list
    if not isinstance(selected_columns_ID, list):
        raise ValueError("selected_columns_ID must be a list")

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    table_name_temp = f"{table_name}_temp"

    with open(COLUMN_DEFS_PATH, "rb") as f:
        column_defs = pickle.load(f)
    escaped_column_defs = [
        f"`{col.split()[0]}` {' '.join(col.split()[1:])}"
        if primary_key not in col
        else col
        for col in column_defs
    ]

    # * Step1: Select based on provided selected_columns_ID
    selected_column_sql_defs = []  # to create the SQL table
    for col in escaped_column_defs:
        if primary_key in col:
            # We will include the primary key directly in the create_table_sql
            continue
        col_int = col.replace("`", "")
        if int(col_int.split("-")[0]) in selected_columns_ID:
            selected_column_sql_defs.append(col)
    selected_column_csv = [primary_key]  # to extract from csv
    for selected_column_sql_def in selected_column_sql_defs:
        if primary_key in selected_column_sql_def:
            continue
        selected_column_sql_def = selected_column_sql_def.replace("`", "")
        selected_column_csv.append(selected_column_sql_def.split(" ")[0])
    print(
        f"There will be {len(selected_column_sql_defs) + 1} columns in the temporary table: {table_name_temp}"
    )

    cursor.execute("PRAGMA foreign_keys = ON;")

    # * Step2: Create the temporary table
    print(f"Creating temporary table: {table_name_temp}")
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name_temp} (
            {primary_key} INTEGER PRIMARY KEY, 
            {', '.join(selected_column_sql_defs)},
            FOREIGN KEY ({primary_key}) REFERENCES {PROCESSED_TABLE_NAME} ({primary_key})
        );
    """
    cursor.execute(create_table_sql)

    # * Step3: Insert the data into the table
    query_eid_sql = f"SELECT {primary_key} FROM {PROCESSED_TABLE_NAME};"
    cursor.execute(query_eid_sql)
    eids = cursor.fetchall()
    eids = [eid[0] for eid in eids]

    print(f"Inserting data into temporary table: {table_name_temp}")
    with tqdm(total=TOTAL_ROWS, desc="Processing CSV", unit="rows") as pbar:
        for chunk in pd.read_csv(
            csv_file_path, chunksize=chunk_size, dtype=str, usecols=selected_column_csv
        ):
            # We only care about those who have exercise ECG data
            chunk[primary_key] = pd.to_numeric(chunk[primary_key], errors="coerce")
            # only keep the rows with desired eids
            chunk = chunk[chunk[primary_key].isin(eids)]

            print(f"Valid Chunk rows: {len(chunk)}")
            # Convert all selected columns to the correct type
            for col in selected_column_csv:
                if col == primary_key:
                    if chunk[col].isna().any():
                        raise ValueError(
                            f"Primary key column '{col}' contains NA values."
                        )
                    chunk[col] = chunk[col].astype(int)
                else:
                    try:
                        chunk[col] = pd.to_numeric(chunk[col], errors="raise")
                        if is_numeric_dtype(chunk[col]):
                            if chunk[col].notna().all() and (chunk[col] % 1 == 0).all():
                                chunk[col] = chunk[col].astype(int)
                            else:
                                chunk[col] = chunk[col].astype(float)
                    except ValueError:
                        chunk[col] = chunk[col].astype(str)

            chunk = chunk.where(pd.notna(chunk), None)

            chunk.to_sql(table_name_temp, conn, if_exists="append", index=False)

            pbar.update(chunk_size)

    # * Step4: Drop all columns that are empty and create the formal table
    print(f"Dropping empty columns in the temporary table: {table_name_temp}")
    cursor.execute(f"PRAGMA table_info({table_name_temp})")
    selected_columns_sql_name = [row[1] for row in cursor.fetchall()]
    non_empty_columns_sql_name = []
    for col_name in tqdm(selected_columns_sql_name):
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM {table_name_temp} 
            WHERE `{col_name}` IS NOT NULL;
        """)
        count = cursor.fetchone()[0]
        if count > 0 or col_name == primary_key:
            non_empty_columns_sql_name.append(col_name)
        else:
            print(f"Dropping empty column: {col_name}")
    print(
        f"There will be {len(non_empty_columns_sql_name)} columns in the table: {table_name}"
    )

    print(f"Creating the formal table: {table_name}")
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            {primary_key} INTEGER PRIMARY KEY,
            {', '.join(f'`{col}`' for col in non_empty_columns_sql_name if col != primary_key)},
            FOREIGN KEY ({primary_key}) REFERENCES {PROCESSED_TABLE_NAME} ({primary_key})
        );
    """)

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
    csv_file_path=CSV_FILE_PATH,
    db_file_path=DB_FILE_PATH,
    chunk_size=5000,
    primary_key=PRIMARY_KEY,
):
    """
    Extract the data from the csv file and update the table based on the selected_columns_ID.
    Only those with ECG data will be updated. This will update an existing table.
    """

    # make sure selected_columns_ID is a list
    if not isinstance(selected_columns_ID, list):
        raise ValueError("selected_columns_ID must be a list")

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    table_name_temp = f"{table_name}_temp"

    # * Step1: Check if the table exists
    cursor.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    )
    if cursor.fetchone() is None:
        raise ValueError(f"Table {table_name} does not exist.")

    with open(COLUMN_DEFS_PATH, "rb") as f:
        column_defs = pickle.load(f)
    escaped_column_defs = [
        f"`{col.split()[0]}` {' '.join(col.split()[1:])}"
        if primary_key not in col
        else col
        for col in column_defs
    ]

    # * Step2: Select based on provided selected_columns_ID
    selected_column_sql_defs = []  # to create the SQL table
    for col in escaped_column_defs:
        if primary_key in col:
            continue
        col_int = col.replace("`", "")
        if int(col_int.split("-")[0]) in selected_columns_ID:
            selected_column_sql_defs.append(col)

    # filter those columns that already exist in the table
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [row[1] for row in cursor.fetchall()]
    selected_column_sql_defs_filtered = [
        col_def
        for col_def in selected_column_sql_defs
        if col_def.split(" ")[0].replace("`", "") not in existing_columns
    ]
    if len(selected_column_sql_defs) - len(selected_column_sql_defs_filtered) > 0:
        print(
            f"{len(selected_column_sql_defs) - len(selected_column_sql_defs_filtered)} columns already exist in the table: {table_name}"
        )
    selected_column_sql_defs = selected_column_sql_defs_filtered
    print(
        f"{len(selected_column_sql_defs)} new columns have been selected."
    )

    selected_column_csv = [primary_key]  # to extract from csv
    for selected_column_sql_def in selected_column_sql_defs:
        if primary_key in selected_column_sql_def:
            continue
        selected_column_sql_def = selected_column_sql_def.replace("`", "")
        selected_column_csv.append(selected_column_sql_def.split(" ")[0])

    cursor.execute("PRAGMA foreign_keys = ON;")

    # * Step2: Create a temporary table
    create_table_sql = f"""
        CREATE TABLE {table_name_temp} (
            {primary_key} INTEGER PRIMARY KEY,
            {', '.join(selected_column_sql_defs)},
            FOREIGN KEY ({primary_key}) REFERENCES {PROCESSED_TABLE_NAME} ({primary_key})
        );
    """
    cursor.execute(create_table_sql)

    # * Step3: Insert the data into the table
    query_eid_sql = f"SELECT {primary_key} FROM {PROCESSED_TABLE_NAME};"
    cursor.execute(query_eid_sql)
    eids = cursor.fetchall()
    eids = [eid[0] for eid in eids]

    print(f"Inserting data into temporary table: {table_name_temp}")
    with tqdm(total=TOTAL_ROWS, desc="Processing CSV", unit="rows") as pbar:
        for chunk in pd.read_csv(
            csv_file_path, chunksize=chunk_size, dtype=str, usecols=selected_column_csv
        ):
            # We only care about those who have exercise ECG data
            chunk[primary_key] = pd.to_numeric(chunk[primary_key], errors="coerce")
            # only keep the rows with desired eids
            chunk = chunk[chunk[primary_key].isin(eids)]

            print(f"Valid Chunk rows: {len(chunk)}")
            # Convert all selected columns to the correct type
            for col in selected_column_csv:
                if col == primary_key:
                    if chunk[col].isna().any():
                        raise ValueError(
                            f"Primary key column '{col}' contains NA values."
                        )
                    chunk[col] = chunk[col].astype(int)
                else:
                    try:
                        chunk[col] = pd.to_numeric(chunk[col], errors="raise")
                        if is_numeric_dtype(chunk[col]):
                            if chunk[col].notna().all() and (chunk[col] % 1 == 0).all():
                                chunk[col] = chunk[col].astype(int)
                            else:
                                chunk[col] = chunk[col].astype(float)
                    except ValueError:
                        chunk[col] = chunk[col].astype(str)

            chunk = chunk.where(pd.notna(chunk), None)

            chunk.to_sql(table_name_temp, conn, if_exists="append", index=False)

            pbar.update(chunk_size)

    # * Step4: Drop all columns that are empty and add to the formal table
    print(f"Dropping empty columns in the temporary table: {table_name_temp}")
    cursor.execute(f"PRAGMA table_info({table_name_temp})")
    selected_columns_sql_name = [row[1] for row in cursor.fetchall()]
    non_empty_columns_sql_name = []
    for col_name in tqdm(selected_columns_sql_name):
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM {table_name_temp} 
            WHERE `{col_name}` IS NOT NULL;
        """)
        count = cursor.fetchone()[0]
        if count > 0 and col_name != primary_key:
            non_empty_columns_sql_name.append(col_name)
        elif count == 0:
            print(f"Dropping empty column: {col_name}")
    print(
        f"There will be {len(non_empty_columns_sql_name)} columns added to the table: {table_name}"
    )

    print(f"Updating the formal table: {table_name}")
    non_empty_columns_sql_defs = []
    for selected_column_sql_def in selected_column_sql_defs:
        if selected_column_sql_def.split(" ")[0].replace("`", "") in non_empty_columns_sql_name:
            non_empty_columns_sql_defs.append(selected_column_sql_def)
    if len(non_empty_columns_sql_defs) > 0:
        for col_def in non_empty_columns_sql_defs:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_def};")
    else:
        print(f"No new columns to add to the table: {table_name}")
        return

    print(f"Inserting data into the formal table: {table_name}")
    update_sql = f"""
        UPDATE {table_name} AS t1
        SET {', '.join(f'`{col}` = (SELECT `{col}` FROM {table_name_temp} t2 WHERE t2.{primary_key} = t1.{primary_key})' 
                      for col in non_empty_columns_sql_name)}        

        WHERE EXISTS (
            SELECT 1 
            FROM {table_name_temp} t2 
            WHERE t2.{primary_key} = t1.{primary_key}
        )
    """
    cursor.execute(update_sql)

    cursor.execute(f"DROP TABLE {table_name_temp}")

    conn.commit()
    conn.close()
    print(f"Table {table_name} has been updated successfully.")