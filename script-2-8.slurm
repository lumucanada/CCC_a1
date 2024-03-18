#!/bin/bash
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-12:00:00

module load mpi4py/3.1.4
module load Python/3.10.4
time mpiexec -n 8 python3 test1.py
my-job-stats -a -n -s
