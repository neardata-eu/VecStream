from time import time

import numpy as np

def search(index, query_vector, topK):
    t_start_search_faiss = time()
    D, I = index.search(query_vector, topK)
    t_end_search_faiss = time()
    timestamps = {
        "start_search_faiss": t_start_search_faiss,
        "end_search_faiss": t_end_search_faiss,
    }
    return D, I, timestamps

def reduce_results(results: list[tuple[np.ndarray, np.ndarray]], top_k=10) -> tuple[np.ndarray, np.ndarray]:
    """
    Reduces multiple sets of search results into a single set of top-k results.
    Args:
        results (list[tuple[np.ndarray, np.ndarray]]): A list of tuples, each containing distances and indices.
        top_k (int): The number of top results to return.
    Returns:
        tuple[np.ndarray, np.ndarray]: A tuple containing the top-k distances and indices.
    """
    if len(results) == 0:
        return np.array([]), np.array([])

    all_distances = np.concatenate([np.atleast_2d(res[0]) for res in results], axis=1)
    all_indices = np.concatenate([np.atleast_2d(res[1]) for res in results], axis=1)
    # all_vectors = np.concatenate([res[2] for res in results])

    if len(all_distances) == 0:
        return np.array([]), np.array([])


    sorted_order = np.argsort(all_distances, axis=1)
    k_top_distances = np.take_along_axis(all_distances, sorted_order[:, :top_k], axis=1)
    k_top_indices = np.take_along_axis(all_indices, sorted_order[:, :top_k], axis=1)
    
    return k_top_distances, k_top_indices