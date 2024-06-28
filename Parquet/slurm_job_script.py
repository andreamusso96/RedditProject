import os


def get_file_string(file_path: str):
    with open(file_path, 'r') as f:
        return f.read()


def analyze_files():
    files = os.listdir('/Users/andrea/Desktop/Temp/slurm_jobs/')
    files = [f for f in files if f.endswith('.out')]
    bad_files = []
    for f in files:
        s = get_file_string(f'/Users/andrea/Desktop/Temp/slurm_jobs/{f}')
        if 'BAD LINES SHARE 1.00' in s:
            file = s.split('\n')[1].split(' ')[8].split('/')[-1]
            print(file)
            bad_files.append(file)

    print(sorted(bad_files))




if __name__ == '__main__':
    analyze_files()
