#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=var_imp_rsf
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=var_imp_cox.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step6_obtain_selected_variables

quarto render Cox_selected_variables.qmd --to pdf