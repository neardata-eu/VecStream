import numpy as np
import io
import time
import json


def load_array(b: bytes) -> np.ndarray:
    """
    Deserializes a NumPy array from a bytes object.

    Args:
        b (bytes): The bytes object containing the serialized NumPy array.

    Returns:
        np.ndarray: The deserialized NumPy array.
    """
    memfile = io.BytesIO(b)
    memfile.seek(0)
    array = np.load(memfile)
    return array

def save_array(array: np.ndarray) -> bytes:
    """
    Serializes a NumPy array into bytes using NumPy's .npy format.

    Args:
        array (np.ndarray): The NumPy array to serialize.

    Returns:
        bytes: The serialized array as bytes.
    """
    memfile = io.BytesIO()
    np.save(memfile, array)
    bytes_data = memfile.getvalue()
    return bytes_data

def deserialize_array(string: str) -> np.ndarray:
    """
    Deserializes a latin1-encoded string representing a NumPy array.
    Args:
        string (str): A latin1-encoded string containing serialized array data.
    Returns:
        np.ndarray: The deserialized NumPy array.
    """
    byte_data = json.loads(string).encode('latin-1')
    array = load_array(byte_data)
    return array
    
    # return pickle.loads(json.loads(string).encode('latin-1'))

def serialize_array(array: np.ndarray) -> str:
    """
    Serializes a NumPy array into a JSON-compatible string.

    The function first converts the array into bytes using `save_array`, decodes the bytes using 'latin-1',
    and then serializes the resulting string into JSON format.

    Args:
        array (np.ndarray): The NumPy array to serialize.

    Returns:
        str: A JSON string representing the serialized array.
    """
    bytes_data = save_array(array).decode('latin-1')
    serialized_as_json = json.dumps(bytes_data)
    return serialized_as_json


    # serialized_as_json = json.dumps(pickle.dumps(array).decode('latin-1'))
    # return serialized_as_json




if __name__ == "__main__":
    # Example usage and simple test
    original_array = np.random.rand(100, 1000).astype(np.float32)
    string = serialize_array(original_array)
    print("Original array shape:", original_array.shape)
    print("Serialized bytes length:", len(string))
    print(f"Serialized latin1 string (first 100 chars): {string[:100]}...")
    reconstructed_array = deserialize_array(string)
    assert np.array_equal(original_array, reconstructed_array)
    print("Serialization and deserialization successful!")


    vector = np.random.rand(100, 1000).astype(np.float32)
    times = []

    for _ in range(1000):
        start = time.time()
        _ = serialize_array(vector)
        # _ = pickle.dumps(vector)
        end = time.time()
        times.append(end - start)

    avg_time = np.mean(times)
    std_time = np.std(times)
    print(f"Average serialization time: {avg_time:.6f} seconds")
    print(f"Standard deviation: {std_time:.6f} seconds")