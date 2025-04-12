import sys

sys.path.append("../..")

from utils.sql_utils import generate_table_from_result_csv
from utils.constants import TableNames, ColumnNames

if __name__ == "__main__":
    # generate_table_from_result_csv(TableNames.HRV_TIME, "../step2_process_ECG/hrv_time_indices.csv")
    # generate_table_from_result_csv(TableNames.HRV_FREQ, "../step2_process_ECG/hrv_frequency_indices.csv")

    # Split nonlinear indices into 3 tables, as the column names can lead to confusion
    generate_table_from_result_csv(
        TableNames.HRV_POINCARE, "../step2_process_ECG/hrv_nonlinear_indices.csv", column_names=ColumnNames.POINCARE_COLUMNS_NAME
    )
    generate_table_from_result_csv(
        TableNames.HRV_ENTROPY, "../step2_process_ECG/hrv_nonlinear_indices.csv", column_names=ColumnNames.ENTROPY_COLUMNS_NAME
    )
    generate_table_from_result_csv(
        TableNames.HRV_FRACTAL, "../step2_process_ECG/hrv_nonlinear_indices.csv", column_names=ColumnNames.FRACTAL_COLUMNS_NAME
    )
