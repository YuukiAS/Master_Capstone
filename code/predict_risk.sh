#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=predict_risk
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=predict_risk.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step7_predict_risk_group

quarto render predict_risk.qmd --to pdf