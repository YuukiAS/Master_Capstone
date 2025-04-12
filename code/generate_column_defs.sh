#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=generate_column_defs
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=generate_column_defs.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/utils
python -u ./csv_utils.py