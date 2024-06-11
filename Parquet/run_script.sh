#!/bin/bash

#SBATCH --nodes=1
#SBATCH --mem-per-cpu=2GB
#SBATCH --time=00:30:00

module load gcc/8.2.0 python/3.11.2

python run_script.py "$@"