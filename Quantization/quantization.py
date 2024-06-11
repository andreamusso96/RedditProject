from sentence_transformers.quantization import quantize_embeddings
import numpy as np
import time


def quantization():
    vals = [4_000_000]
    times = []
    for i in range(len(vals)):
        embeddings = np.random.uniform(-1, 1, size=(vals[i], 312))
        t1 = time.time()
        binary_embeddings = quantize_embeddings(embeddings, precision="int8")
        t2 = time.time()
        print(f"Quantization time: {t2 - t1}")
        times.append(t2 - t1)

    return vals, times


if __name__ == '__main__':
    import pandas as pd
    v, t = quantization()
    df = pd.DataFrame({'vals': v, 'times': t})
    df.to_csv('quantization.csv')

    df = pd.read_csv('quantization.csv')
    import plotly.graph_objects as go
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['vals'], y=df['times'], mode='lines+markers'))
    fig.show()
