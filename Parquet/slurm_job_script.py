import os


def get_file_string(file_path: str):
    with open(file_path, 'r') as f:
        return f.read()


def analyze_files():
    files = os.listdir('/Users/andrea/Desktop/Temp/slurm_jobs/')
    files = [f for f in files if f.endswith('.out')]
    for f in files:
        s = get_file_string(f'/Users/andrea/Desktop/Temp/slurm_jobs/{f}')
        if 'RS_2022' in s:
            print(s)




if __name__ == '__main__':
    analyze_files()
