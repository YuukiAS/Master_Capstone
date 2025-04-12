#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=extract_HRV
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=extract_HRV.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step2_process_ECG

python -u ./extract_HRV.py