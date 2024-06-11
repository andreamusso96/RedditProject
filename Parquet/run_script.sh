#!/bin/bash

#SBATCH --job-name=my_job_name
#SBATCH --nodes=1
#SBATCH --gpus=rtx_3090:1
#SBATCH --mem-per-cpu=150GB
#SBATCH --time=08:30:00
#SBATCH --output=output.txt
#SBATCH --error=error.txt

# module load gcc/8.2.0 python_gpu/3.11.2

python run_script.py "$@"