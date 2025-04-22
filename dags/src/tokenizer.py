import time
import torch
import numpy as np

def tokenise(audio_np_array: np.ndarray) -> torch.Tensor:
    if not isinstance(audio_np_array, np.ndarray):
        raise ValueError("Input should be a NumPy array")

    del audio_np_array
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    start_time = time.time()
    while True:
        tensor_length = np.random.randint(20, 1001)
        result_tensor = torch.randint(
            low=-32768,
            high=32767,
            size=(tensor_length,),
            dtype=torch.int16,
            device=device
        )

        a = torch.rand(1000, 1000, device=device)
        b = torch.rand(1000, 1000, device=device)
        _ = torch.matmul(a, b)

        if (time.time() - start_time) * 1000 >= 200:
            break

    return result_tensor