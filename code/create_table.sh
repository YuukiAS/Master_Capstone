#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=create_table
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=create_table.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/utils
python -u ./sql_utils.py