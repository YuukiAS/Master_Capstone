import pandas as pd
from tqdm import tqdm
import sys

sys.path.append("../../")

from utils.constants import DatabaseConfig
from utils.ecg_processor import ECG_Processor
from utils.sql_utils import query_eids

if __name__ == "__main__":
    time_indices = []
    freq_indices = []
    nonlinear_indices = []

    # eids = query_eids()
    eids = [1017798]

    cnt_total = len(eids)
    cnt_success = 0

    for eid in tqdm(eids, desc="Extracting HRV indices"):
        
        try:
            ecg_processor = ECG_Processor(
                data_dir=DatabaseConfig.ECG_FOLDER, subject=eid
            )
            hrv_time, hrv_freq, hrv_nonlinear = ecg_processor.process_signal("noload")

            hrv_time["eid"] = eid
            hrv_freq["eid"] = eid
            hrv_nonlinear["eid"] = eid

            time_indices.append(hrv_time)
            freq_indices.append(hrv_freq)
            nonlinear_indices.append(hrv_nonlinear)

            cnt_success += 1
        except ValueError as e:
            print(f"Error processing ECG data for eid {eid}: {e}")
            continue

    time_df = pd.concat(time_indices, ignore_index=True)
    freq_df = pd.concat(freq_indices, ignore_index=True)
    nonlinear_df = pd.concat(nonlinear_indices, ignore_index=True)
    # Move eid to the first column
    time_df = time_df[["eid"] + [col for col in time_df.columns if col != "eid"]]
    freq_df = freq_df[["eid"] + [col for col in freq_df.columns if col != "eid"]]
    nonlinear_df = nonlinear_df[
        ["eid"] + [col for col in nonlinear_df.columns if col != "eid"]
    ]

    time_df.to_csv("hrv_time_indices.csv", index=False)
    freq_df.to_csv("hrv_freq_indices.csv", index=False)
    nonlinear_df.to_csv("hrv_nonlinear_indices.csv", index=False)

    print(
        f"Successfully extracted HRV indices for {cnt_success}/{cnt_total} -> {(cnt_success / cnt_total * 100):.2f}%"
    )
