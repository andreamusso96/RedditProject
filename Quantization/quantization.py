from sentence_transformers.quantization import quantize_embeddings
import numpy as np
import time
import h5py
import logging

logger = logging.getLogger('quantization')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


def load_data(file_path: str):
    hf = h5py.File(file_path, 'r')
    n_datasets = len(hf.keys())

    hf0 = hf[list(hf.keys())[0]]
    size_vector = hf0.shape[1]
    size_dataset = n_datasets * hf0.shape[0]
    dataset = np.zeros((size_dataset, size_vector))

    counter = 0
    for name in hf.keys():
        ds = hf[name]
        size_ds = ds.shape[0]
        dataset[counter:counter+size_ds] = ds
        counter += size_ds

    hf.close()
    dataset = dataset[:counter]
    return dataset


def quantization(data: np.ndarray):
    quantized_embeddings = quantize_embeddings(data, precision="int8")
    return quantized_embeddings


def save_quantized_embeddings(file_path_embeddings: str, file_path_quantized_embeddings: str):
    logger.info(f'Loading embeddings from {file_path_embeddings}')
    data = load_data(file_path_embeddings)

    logger.info('Quantizing embeddings')
    quantized_embeddings = quantization(data)

    logger.info('Saving quantized embeddings')
    batch_size = 10000
    with h5py.File(file_path_quantized_embeddings, 'w') as hf:
        for i in range(0, len(quantized_embeddings), batch_size):
            hf.create_dataset(f'{i}', data=quantized_embeddings[i:i+batch_size])

    logger.info('Done saving quantized embeddings')


if __name__ == '__main__':
    # fp_embeddings = '/Users/andrea/Desktop/PhD/Projects/Current/Reddit/data/embeddings_submissions_all-mpnet-base-v2.h5py'
    # fp_quantized_embeddings = '/Users/andrea/Desktop/PhD/Projects/Current/Reddit/data/quantized_embeddings_submissions_all-mpnet-base-v2.h5py'
    fp_embeddings = '/cluster/work/coss/anmusso/victoria/embeddings/embeddings_submissions_all-MiniLM-L6-v2.h5py'
    fp_quantized_embeddings = '/cluster/work/coss/anmusso/victoria/embeddings/quantized_embeddings_submissions_all-MiniLM-L6-v2.h5py'
    save_quantized_embeddings(fp_embeddings, fp_quantized_embeddings)



