from typing import List
import os

import pandas as pd


def extract_zst_file_names(folder_path: str) -> List[str]:
    file_names = os.listdir(folder_path)
    file_names = [f.split('.')[0] for f in file_names if f.startswith('RS')]
    return file_names


def launch_jobs():
    # zst_folder_path = '/Users/andrea/Desktop/PhD/Projects/Current/Reddit/data/comments'
    # parquet_folder_path = '/Users/andrea/Desktop/PhD/Projects/Current/Reddit/data/parquet_test'
    zst_folder_path = '/cluster/work/coss/anmusso/reddit/submissions'
    parquet_folder_path = '/cluster/work/coss/anmusso/reddit_parquet/submissions'
    file_names = extract_zst_file_names(folder_path=zst_folder_path)
    file_names = sorted(file_names)[:1]
    print(file_names)

    os.system('chmod +x run_script.sh')
    for file_name in file_names:

        os.system(f'sbatch run_script.sh {file_name} {zst_folder_path} {parquet_folder_path}')
        # os.system(f'./run_script.sh {file_name} {zst_folder_path} {parquet_folder_path}')


if __name__ == '__main__':
    launch_jobs()