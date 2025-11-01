from time import time
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from tqdm.notebook import tqdm

from dtos.model_dtos.model_config_dto import ModelConfigDto
from dtos.model_dtos.model_output_dto import ModelOutputDto
from logger.logger import Logger
from train_test_creator.train_test_set import TrainTestSet


class TimeSeriesDataset(Dataset):
    def __init__(self, windows, input_window, horizon):
        self.X, self.y = [], []

        for window in windows:
            data = window.values
            if len(data) < input_window + horizon:
                continue  # skip incomplete window

            self.X.append(data[:input_window, :-1])
            self.y.append(data[input_window : input_window + horizon, -1])

        self.X = torch.tensor(np.array(self.X), dtype=torch.float32)
        self.y = torch.tensor(np.array(self.y), dtype=torch.float32)

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]


class LSTMForecastModel(nn.Module):
    def __init__(self, num_features, hidden_size, num_layers, forecast_horizon):
        super(LSTMForecastModel, self).__init__()
        self.lstm = nn.LSTM(
            input_size=num_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=0.2,
        )

        self.fc = nn.Sequential(
            nn.Linear(hidden_size, 64), nn.ReLU(), nn.Linear(64, forecast_horizon)
        )

    def forward(self, x):
        output, _ = self.lstm(x)  # (batch, seq_len, hidden_size)
        last_output = output[:, -1, :]  # take last time step
        return self.fc(last_output)


class LSTM_Model:
    def __init__(
        self, logger: Logger, train_test_set: TrainTestSet, model_config: ModelConfigDto
    ):
        self.logger = logger
        self._train_test_set = train_test_set
        self._model_config = model_config

        self._validate_model_config()

        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.logger.log_info(f"PyTorch version: {torch.__version__}")
        self.logger.log_info(f"Using device: {self._device}")

        print(f"PyTorch version: {torch.__version__}")
        print(f"Using device: {self._device}")

    def _validate_model_config(self):
        """
        Validate the model configuration parameters.
        Raises:
            ValueError: if any configuration value is invalid.
        """
        cfg = self._model_config

        # epochs: positive integer
        if not isinstance(cfg.epochs, int) or cfg.epochs <= 0:
            raise ValueError(
                f"Invalid epochs ({cfg.epochs}): must be a positive integer."
            )

        # learning_rate: positive float, usually < 1
        if not isinstance(cfg.learning_rate, (float, int)) or cfg.learning_rate <= 0:
            raise ValueError(
                f"Invalid learning_rate ({cfg.learning_rate}): must be positive."
            )
        if cfg.learning_rate > 1:
            raise ValueError(
                f"Suspicious learning_rate ({cfg.learning_rate}): should usually be < 1."
            )

        # batch_size: positive integer
        if not isinstance(cfg.batch_size, int) or cfg.batch_size <= 0:
            raise ValueError(
                f"Invalid batch_size ({cfg.batch_size}): must be a positive integer."
            )

        # Optionally check batch size divisibility if data size is known
        train_len = getattr(self._train_test_set, "train_X", None)
        if train_len is not None:
            n_samples = len(train_len)
            if cfg.batch_size > n_samples:
                raise ValueError(
                    f"batch_size ({cfg.batch_size}) cannot exceed number of training samples ({n_samples})."
                )

        self.logger.log_info("Model configuration validated successfully.")

    def train(self):
        start_time = time()
        self.logger.log_info("Starting training process...")

        # Training logic would go here
        train_dataset = TimeSeriesDataset(
            self._train_test_set.train_set,
            self._train_test_set.input_window_size,
            self._train_test_set.forecast_horizon_size,
        )

        train_loader = DataLoader(
            train_dataset, batch_size=self._model_config.batch_size, shuffle=False
        )

        num_features = train_dataset.X.shape[-1]
        model = LSTMForecastModel(
            num_features,
            hidden_size=128,
            num_layers=2,
            forecast_horizon=self._train_test_set.forecast_horizon_size,
        ).to(self._device)

        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(
            model.parameters(), lr=self._model_config.learning_rate
        )

        print(f"Training on {self._device} for {self._model_config.epochs} epochs...\n")

        for epoch in range(self._model_config.epochs):
            model.train()
            running_loss = 0.0
            progress_bar = tqdm(
                train_loader,
                desc=f"Epoch {epoch+1}/{self._model_config.epochs}",
                leave=False,
            )

            for X_batch, y_batch in progress_bar:
                X_batch, y_batch = X_batch.to(self._device), y_batch.to(self._device)

                optimizer.zero_grad()
                y_pred = model(X_batch)
                loss = criterion(y_pred, y_batch)
                loss.backward()
                optimizer.step()

                running_loss += loss.item()
                progress_bar.set_postfix(loss=loss.item())

            avg_loss = running_loss / len(train_loader)
            print(
                f"Epoch [{epoch+1}/{self._model_config.epochs}] - Avg Train Loss: {avg_loss:.6f}"
            )

        # --- Evaluate ---
        model.eval()
        with torch.no_grad():
            test_data = self._train_test_set.test_set.values
            horizon = self._train_test_set.forecast_horizon_size

            y_true = (
                torch.tensor(test_data[:, -1], dtype=torch.float32)
                .unsqueeze(0)
                .to(self._device)
            )
            last_train_window = self._train_test_set.train_set[-1].values
            X_input = (
                torch.tensor(
                    last_train_window[: self._train_test_set.input_window_size, :-1],
                    dtype=torch.float32,
                )
                .unsqueeze(0)
                .to(self._device)
            )

            y_pred = model(X_input)

            # Denormalize predictions
            y_pred_denorm = (
                y_pred
                * (
                    self._train_test_set.output_range[1]
                    - self._train_test_set.output_range[0]
                )
                + self._train_test_set.output_range[0]
            )
            y_true_denorm = (
                y_true
                * (
                    self._train_test_set.output_range[1]
                    - self._train_test_set.output_range[0]
                )
                + self._train_test_set.output_range[0]
            )

            test_loss = criterion(y_pred_denorm, y_true_denorm).item()

        training_time = time() - start_time

        print(f"\n✅ Test MSE (forecast horizon = {horizon}): {test_loss:.6f}")
        print("Training completed.")

        self.logger.log_info(
            f"\n✅ Test MSE (forecast horizon = {horizon}): {test_loss:.6f}"
        )
        self.logger.log_info("Training completed.")

        # --- Return metadata ---
        model_output = ModelOutputDto(
            model=model,
            model_state_dict=model.state_dict(),
            model_config=self._model_config,
            train_loss_history=[],  # optionally fill this during training
            final_train_loss=avg_loss,
            test_loss=test_loss,
            y_pred=y_pred.cpu().numpy().flatten(),
            y_pred_denorm=y_pred_denorm.cpu().numpy().flatten(),
            y_true=y_true.cpu().numpy().flatten(),
            y_true_denorm=y_true_denorm.cpu().numpy().flatten(),
            input_window_size=self._train_test_set.input_window_size,
            horizon_size=horizon,
            training_time=training_time,
        )

        return model_output
