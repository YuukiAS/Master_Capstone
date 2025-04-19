#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=xgb_full
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=xgb_full.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step4_build_survival_model/XGBoost

quarto render XGBoost_full.qmd --to pdf