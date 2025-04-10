import csv
import pandas as pd
import dask.dataframe as dd
from IPython.display import display, HTML

FILE_PATH = "/work/users/y/u/yuukias/database/UKBiobank/ukb673099_Jun2023.csv"

def count_row_numbers(file_path=FILE_PATH):
    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        row_count = sum(1 for row in reader)
    return row_count

def get_column_names(file_path=FILE_PATH):
    with open(file_path, "r", newline="") as csvfile:
        reader = csv.reader(csvfile)
        column_names = next(reader)
    return column_names

def get_data(
    target_column_names, file_path=FILE_PATH, eids=None
):
    dtype_objects = {"eid": "int64"}
    for col_name in target_column_names:
        dtype_objects[col_name] = "object"

    df = dd.read_csv(
        file_path,
        usecols=["eid", *target_column_names],
        header=0,
        sample=1000000,
        dtype=dtype_objects,
    ).compute()

    if eids is not None:
        if not isinstance(eids, list):
            raise ValueError("eids must be a list")
        df = df[df["eid"].isin(eids)]

    # Convert columns to numeric if possible
    for col in df.columns:
        if col != "eid" and df[col].dtype == 'string':
            try:
                converted = pd.to_numeric(df[col])
                df[col] = converted
            except ValueError:
                pass

    # Drop all rows with all NA values, even if the row is in the eids list
    cols_to_drop = [col for col in df.columns if col != 'eid' and df[col].isna().all()]
    df = df.drop(columns=cols_to_drop)
    return df


def peek_data(
    target_column_names, file_path=FILE_PATH, eids=None
):
    dtype_objects = {"eid": "int64"}
    for col_name in target_column_names:
        dtype_objects[col_name] = "object"

    df = dd.read_csv(
        file_path,
        usecols=["eid", *target_column_names],
        header=0,
        sample=1000000,
        dtype=dtype_objects
    ).compute()

    if eids is not None:
        if not isinstance(eids, list):
            raise ValueError("eids must be a list")
        df = df[df["eid"].isin(eids)]
    
    # Convert columns to numeric if possible
    for col in df.columns:
        if col != "eid" and df[col].dtype == 'string':
            try:
                converted = pd.to_numeric(df[col])
                df[col] = converted
            except ValueError:
                pass

    # Drop all rows with all NA values, even if the row is in the eids list
    cols_to_drop = [col for col in df.columns if col != 'eid' and df[col].isna().all()]
    df = df.drop(columns=cols_to_drop)

    display(HTML(df.head(100).to_html()))
