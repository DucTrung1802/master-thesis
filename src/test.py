import torch

print("PyTorch version:", torch.__version__)
print("CUDA available:", torch.cuda.is_available())
print("CUDA version (PyTorch):", torch.version.cuda)
if torch.cuda.is_available():
    print("GPU Name:", torch.cuda.get_device_name(0))
