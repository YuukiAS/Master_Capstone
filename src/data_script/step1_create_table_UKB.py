import sys

sys.path.append("../..")

from utils.sql_utils import generate_table_from_main_csv
from utils.constants import TableNames, ColumnIDs

if __name__ == "__main__":
    # generate_table_from_main_csv(TableNames.CONFOUNDERS, ColumnIDs.CONFOUNDER_COLUMNS_ID)
    # generate_table_from_main_csv(TableNames.ECG, ColumnIDs.ECG_COLUMNS_ID)
    generate_table_from_main_csv(TableNames.ICD, ColumnIDs.ICD_COLUMNS_ID)