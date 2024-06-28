#!/bin/bash

#SBATCH --nodes=1
#SBATCH --mem-per-cpu=5GB
#SBATCH --time=00:50:00

module load stack/2024-06 python/3.11.6

python run_script.py "$@"