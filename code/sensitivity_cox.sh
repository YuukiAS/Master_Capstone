#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=sensitivity_cox
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=sensitivity_cox.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step8_sensitivity_analysis

quarto render sensitivity_Cox.qmd --to pdf