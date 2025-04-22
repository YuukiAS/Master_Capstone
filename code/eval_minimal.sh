#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=eval_minimal
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=eval_minimal.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step5_compare_model_performance

quarto render evaluate_performance_minimal.qmd --to pdf