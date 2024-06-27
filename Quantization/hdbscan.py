from sklearn.cluster import HDBSCAN
import numpy as np

def sample_data():
    n_clusters = 700
    n_samples_per_cluster = 40_000_000 // n_clusters

    centers = np.random.uniform(-10, 10, (n_clusters, 5))
    samples = []
    for i in range(n_clusters):
        gaussian = np.random.normal(0, 10, (n_samples_per_cluster, 5))
        sample = gaussian + centers[i]
        samples.append(sample)

    samples = np.vstack(samples)
    return samples


def hdbscan(samples):
    clusterer = HDBSCAN(min_cluster_size=100, min_samples=30)
    print('Fitting')
    clusterer.fit(samples)
    print('DONE')
    res = clusterer.labels_
    return clusterer, res

if __name__ == '__main__':
    hdbscan(sample_data())
