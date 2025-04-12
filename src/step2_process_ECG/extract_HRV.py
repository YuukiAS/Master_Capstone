import pandas as pd
from tqdm import tqdm
import sys
from multiprocessing import Pool, cpu_count
from functools import partial

sys.path.append("../../")

from utils.constants import DatabaseConfig
from utils.ecg_processor import ECG_Processor
from utils.sql_utils import query_eids


def process_single_subject(eid, data_dir):
    """Process a single subject's ECG data"""
    try:
        ecg_processor = ECG_Processor(data_dir=data_dir, subject=str(eid))
        hrv_time, hrv_freq, hrv_nonlinear = ecg_processor.process_signal("noload")

        # Add eid to each DataFrame
        hrv_time["eid"] = eid
        hrv_freq["eid"] = eid
        hrv_nonlinear["eid"] = eid

        return {
            "success": True,
            "eid": eid,
            "time": hrv_time,
            "freq": hrv_freq,
            "nonlinear": hrv_nonlinear,
        }
    except Exception as e:
        return {"success": False, "eid": eid, "error": str(e)}


if __name__ == "__main__":
    try:
        eids = query_eids()
        if not eids:
            raise ValueError("No eids found in database")

        cnt_total = len(eids)

        # Setup parallel processing
        n_cores = max(1, cpu_count() - 1)  # Leave one core free
        print(f"Using {n_cores} cores for parallel processing")

        # Create partial function with fixed data_dir
        process_func = partial(
            process_single_subject, data_dir=DatabaseConfig.ECG_FOLDER
        )

        time_indices = []
        freq_indices = []
        nonlinear_indices = []
        cnt_processed = 0
        cnt_success = 0

        with Pool(n_cores) as pool:
            pbar = tqdm(
                pool.imap_unordered(process_func, eids),
                total=cnt_total,
                desc="Extracting HRV indices",
            )
            for result in pbar:
                cnt_processed += 1
                if result["success"]:
                    time_indices.append(result["time"])
                    freq_indices.append(result["freq"])
                    nonlinear_indices.append(result["nonlinear"])
                    cnt_success += 1

                    pbar.set_postfix(
                        {
                            "success": f"{cnt_success}/{cnt_processed}",
                            "rate": f"{(cnt_success / cnt_processed * 100):.1f}%",
                        }
                    )
                else:
                    print(
                        f"Error processing ECG data for eid {result['eid']}: {result['error']}"
                    )

        if cnt_success == 0:
            raise ValueError("No ECG data was successfully processed")

        # Combine all results
        time_df = pd.concat(time_indices, ignore_index=True)
        freq_df = pd.concat(freq_indices, ignore_index=True)
        nonlinear_df = pd.concat(nonlinear_indices, ignore_index=True)

        # Reorder columns to put eid first
        time_df = time_df[["eid"] + [col for col in time_df.columns if col != "eid"]]
        freq_df = freq_df[["eid"] + [col for col in freq_df.columns if col != "eid"]]
        nonlinear_df = nonlinear_df[
            ["eid"] + [col for col in nonlinear_df.columns if col != "eid"]
        ]
        print(
            f"\nHRV indices extracted for {cnt_success}/{cnt_total} subjects -> {(cnt_success / cnt_total * 100):.2f}%"
        )

        time_df.to_csv("hrv_time_indices.csv", index=False)
        freq_df.to_csv("hrv_frequency_indices.csv", index=False)
        nonlinear_df.to_csv("hrv_nonlinear_indices.csv", index=False)

        print("Extracted HRV indices are saved to CSV files")

    except Exception as e:
        print(f"Error when extracting HRV indices: {str(e)}")
        sys.exit(1)
