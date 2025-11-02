# # cnn_model.py
# """
# Basic CNN model for time series forecasting using PyTorch.

# Assumptions:
# - Input shape: (batch_size, input_window_size, num_features)
# - Output shape: (batch_size, test_window_size)
# """

# import numpy as np
# import pandas as pd
# from typing import List
# import torch
# import torch.nn as nn
# from torch.utils.data import Dataset, DataLoader


# # ============================================================
# # Data handling
# # ============================================================


# class TrainTestSet:
#     def __init__(
#         self,
#         name: str,
#         train_set: List[pd.DataFrame],
#         test_set: pd.DataFrame,
#         input_window_size: int,
#         test_window_size: int,
#     ):
#         self.name = name
#         self.input_window_size = input_window_size
#         self.test_window_size = test_window_size
#         self.train_set = train_set
#         self.test_set = test_set

#     def get_number_of_train_windows(self) -> int:
#         return len(self.train_set)


# class TimeSeriesDataset(Dataset):
#     """Custom Dataset for time series sliding windows."""

#     def __init__(self, windows: List[pd.DataFrame], input_window: int, horizon: int):
#         self.X = []
#         self.y = []

#         for window in windows:
#             data = window.values
#             self.X.append(data[:input_window, :-1])  # all features except 'close'
#             self.y.append(data[input_window:, -1])  # forecast target

#         self.X = torch.tensor(np.array(self.X), dtype=torch.float32)
#         self.y = torch.tensor(np.array(self.y), dtype=torch.float32)

#     def __len__(self):
#         return len(self.X)

#     def __getitem__(self, idx):
#         return self.X[idx], self.y[idx]


# # ============================================================
# # CNN Model Definition
# # ============================================================


# class CNNForecastModel(nn.Module):
#     def __init__(self, num_features: int, input_window: int, forecast_horizon: int):
#         super(CNNForecastModel, self).__init__()

#         self.network = nn.Sequential(
#             nn.Conv1d(
#                 in_channels=num_features,
#                 out_channels=64,
#                 kernel_size=3,
#                 padding="causal",
#             ),
#             nn.ReLU(),
#             nn.Conv1d(in_channels=64, out_channels=64, kernel_size=3, padding="causal"),
#             nn.ReLU(),
#             nn.MaxPool1d(kernel_size=2),
#             nn.Conv1d(
#                 in_channels=64, out_channels=128, kernel_size=3, padding="causal"
#             ),
#             nn.ReLU(),
#             nn.AdaptiveAvgPool1d(1),  # Global average pooling
#         )

#         self.fc = nn.Sequential(
#             nn.Flatten(),
#             nn.Linear(128, 64),
#             nn.ReLU(),
#             nn.Dropout(0.3),
#             nn.Linear(64, forecast_horizon),
#         )

#     def forward(self, x):
#         # x: (batch, seq_len, num_features)
#         x = x.permute(0, 2, 1)  # -> (batch, num_features, seq_len)
#         x = self.network(x)
#         out = self.fc(x)
#         return out


# # ============================================================
# # Training / Evaluation Functions
# # ============================================================


# def train_model(
#     train_test_set: TrainTestSet,
#     epochs: int = 50,
#     batch_size: int = 16,
#     lr: float = 1e-3,
# ):
#     device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

#     # Prepare datasets
#     train_dataset = TimeSeriesDataset(
#         train_test_set.train_set,
#         train_test_set.input_window_size,
#         train_test_set.test_window_size,
#     )
#     test_dataset = TimeSeriesDataset(
#         [train_test_set.test_set],
#         train_test_set.input_window_size,
#         train_test_set.test_window_size,
#     )

#     train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
#     test_loader = DataLoader(test_dataset, batch_size=1, shuffle=False)

#     num_features = train_dataset.X.shape[-1]
#     model = CNNForecastModel(
#         num_features,
#         train_test_set.input_window_size,
#         train_test_set.test_window_size,
#     ).to(device)

#     criterion = nn.MSELoss()
#     optimizer = torch.optim.Adam(model.parameters(), lr=lr)

#     for epoch in range(epochs):
#         model.train()
#         running_loss = 0.0

#         for X_batch, y_batch in train_loader:
#             X_batch, y_batch = X_batch.to(device), y_batch.to(device)

#             optimizer.zero_grad()
#             y_pred = model(X_batch)
#             loss = criterion(y_pred, y_batch)
#             loss.backward()
#             optimizer.step()

#             running_loss += loss.item()

#         avg_loss = running_loss / len(train_loader)
#         print(f"Epoch [{epoch + 1}/{epochs}], Train Loss: {avg_loss:.6f}")

#     # Evaluate
#     model.eval()
#     with torch.no_grad():
#         test_losses = []
#         for X_batch, y_batch in test_loader:
#             X_batch, y_batch = X_batch.to(device), y_batch.to(device)
#             y_pred = model(X_batch)
#             loss = criterion(y_pred, y_batch)
#             test_losses.append(loss.item())

#         test_loss = np.mean(test_losses)
#         print(f"\nTest MSE: {test_loss:.6f}")

#     return model


# # ============================================================
# # Example usage
# # ============================================================

# if __name__ == "__main__":
#     num_features = 220 + 1  # 220 features + 'close'
#     input_window = 360
#     forecast_horizon = 30

#     # Create dummy data
#     train_windows = [
#         pd.DataFrame(
#             np.random.rand(input_window + forecast_horizon, num_features),
#             columns=[f"f{i}" for i in range(num_features - 1)] + ["close"],
#         )
#         for _ in range(80)
#     ]

#     test_window = pd.DataFrame(
#         np.random.rand(input_window + forecast_horizon, num_features),
#         columns=[f"f{i}" for i in range(num_features - 1)] + ["close"],
#     )

#     dataset = TrainTestSet(
#         name="example",
#         train_set=train_windows,
#         test_set=test_window,
#         input_window_size=input_window,
#         test_window_size=forecast_horizon,
#     )

#     trained_model = train_model(dataset)
