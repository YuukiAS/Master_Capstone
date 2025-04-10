#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=count_valid_ECG
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=count_valid_ECG.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step0_prepare_data
python -u ./count_valid_ECG.py