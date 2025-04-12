#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=impute_data
#SBATCH --cpus-per-task=32
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=impute_data.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step3_impute_split_data/impute_data

quarto render impute_data_MissForest_eligible.qmd --to pdf
# quarto render impute_data_MissForest_full.qmd --to pdf