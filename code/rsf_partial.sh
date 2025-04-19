#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --job-name=rsf_partial
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --partition=general
#SBATCH --output=rsf_partial.out

cd /work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step4_build_survival_model/RSF

quarto render RSF_partial.qmd --to pdf