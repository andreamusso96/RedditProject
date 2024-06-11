import numpy as np
import pandas as pd
from sklearn.cluster import HDBSCAN

def fake_data():
    n_clusters = 700
    n_vectors_per_cluster = 2_000_000 // n_clusters

    base_vectors = np.random.uniform(0, 30, size=(n_clusters, 5))
    fake_data = []
    for v in base_vectors:
        gaus = np.random.normal(0, 3, size=(n_vectors_per_cluster, 5))
        sample = v + gaus
        fake_data.append(sample)

    fake_data = np.vstack(fake_data)
    return fake_data

def true_data():
    data = pd.read_parquet('/Users/andrea/Desktop/PhD/Projects/Current/Reddit/data/submissions_coordinates.parquet')
    data = data.iloc[:,:-1]
    return data


def test(data):
    scanner = HDBSCAN(min_cluster_size=100)
    clusters = scanner.fit_predict(data)
    return clusters


if __name__ == '__main__':
    d = true_data()
    test(d)



