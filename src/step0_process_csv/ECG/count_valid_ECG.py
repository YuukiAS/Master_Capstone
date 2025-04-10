import numpy as np
from tqdm import tqdm
import os
import sys

sys.path.append("../..")
from utils.ecg_processor import ECG_Processor

ECG_folder = "/users/y/u/yuukias/database/UKBiobank/6025"

cnt_total = cnt_valid = 0
for ID in tqdm(sorted(os.listdir(ECG_folder))):
    print(f"{cnt_valid} / {cnt_total}")
    ID = ID.split("_")[0]
    cnt_total += 1
    try:
        ecg_processor_temp = ECG_Processor(ECG_folder, ID)
        cnt_valid += 1
    except ValueError as e:
        print(f"{ID}: {e}")

print(f"{cnt_valid} / {cnt_total}")