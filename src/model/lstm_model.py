import json
import os
from time import time
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from tqdm.notebook import tqdm
import wandb
from dotenv import load_dotenv
import pickle

from dtos.model_dtos.model_config_dto import ModelConfigDto
from dtos.model_dtos.model_output_dto import ModelOutputDto
from logger.logger import Logger
from train_test_creator.train_test_set import TrainTestSet
from utils.constants import PATIENCE
from utils.enums import (
    AchitectureType,
    LossFunctionType,
    MetricType,
    OptimizerType,
    ScalerType,
)

load_dotenv()


# =============================
# DATASET
# =============================
class TimeSeriesDataset(Dataset):
    """
    Each window is divided into:
        - Training segment: train_window_size (input)
        - Validation segment: validation_window_size (input)
        - Testing segment: test_window_size (target)
    """

    def __init__(
        self, windows, train_window_size, validation_window_size, test_window_size
    ):
        self.X_train, self.y_train = [], []
        self.X_val, self.y_val = [], []

        for window in windows:
            data = window.values
            total_needed = train_window_size + validation_window_size + test_window_size
            if len(data) < total_needed:
                continue

            # --- TRAIN segment
            self.X_train.append(data[:train_window_size, :-1])
            self.y_train.append(
                data[train_window_size : train_window_size + test_window_size, -1]
            )

            # --- VALIDATION segment
            val_end = train_window_size + validation_window_size
            self.X_val.append(data[val_end - train_window_size : val_end, :-1])
            self.y_val.append(data[val_end : val_end + test_window_size, -1])

        # Convert to tensors
        self.X_train = torch.tensor(np.array(self.X_train), dtype=torch.float32)
        self.y_train = torch.tensor(np.array(self.y_train), dtype=torch.float32)
        self.X_val = torch.tensor(np.array(self.X_val), dtype=torch.float32)
        self.y_val = torch.tensor(np.array(self.y_val), dtype=torch.float32)

    def __len__(self):
        return len(self.X_train)

    def __getitem__(self, idx):
        return self.X_train[idx], self.y_train[idx]


# =============================
# MODEL
# =============================
class LSTMForecastModel(nn.Module):
    def __init__(self, num_features, hidden_size, num_layers, test_window_size):
        super(LSTMForecastModel, self).__init__()
        self.lstm = nn.LSTM(
            input_size=num_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=0.2,
        )
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, 64), nn.ReLU(), nn.Linear(64, test_window_size)
        )

    def forward(self, x):
        output, _ = self.lstm(x)
        last_output = output[:, -1, :]
        return self.fc(last_output)


# =============================
# MAIN CLASS
# =============================
class LSTM_Model:
    def __init__(
        self, logger: Logger, train_test_set: TrainTestSet, model_config: ModelConfigDto
    ):
        self._logger = logger

        if not self._validate_model_config_dto(model_config):
            self._logger.log_error("Invalid ModelConfigDto")
            raise ValueError("Invalid ModelConfigDto")

        self._train_test_set = train_test_set
        self._model_config = model_config
        self._model_config.architecture = AchitectureType.LSTM
        self._device = torch.device(model_config.device)

        self._logger.log_info(f"PyTorch version: {torch.__version__}")
        print(f"PyTorch version: {torch.__version__}")

        # Initialize WandB
        wandb.login(key=os.getenv("WANDB_KEY"))
        self._run = wandb.init(
            entity=self._model_config.entity,
            project=self._model_config.project,
            config=self._model_config.to_dict(),
        )

    # --------------------------
    # CONFIG VALIDATION
    # --------------------------
    def _validate_model_config_dto(self, model_config: ModelConfigDto) -> bool:
        errors = []

        if not isinstance(model_config, ModelConfigDto):
            raise TypeError("model_config must be an instance of ModelConfigDto.")

        if not model_config.entity:
            errors.append("Entity must not be empty.")
        if not model_config.project:
            errors.append("Project must not be empty.")
        if not model_config.stock_code:
            errors.append("Stock code must not be empty.")

        if model_config.train_window_size <= 0:
            errors.append("Train window size must be greater than 0.")
        if model_config.test_window_size <= 0:
            errors.append("Test window size must be greater than 0.")
        if model_config.epochs <= 0:
            errors.append("Epochs must be greater than 0.")
        if model_config.batch_size <= 0:
            errors.append("Batch size must be greater than 0.")
        if not (0 < model_config.learning_rate <= 1):
            errors.append("Learning rate must be between 0 and 1.")

        if errors:
            raise ValueError(
                "Invalid model configuration:\n"
                + "\n".join(f"- {err}" for err in errors)
            )
        return True

    # --------------------------
    # TRAIN FUNCTION
    # --------------------------
    def train(self):
        self._logger.log_info("START TRAINING PROCESS...")
        output_range = self._train_test_set.output_range
        start_time = time()

        # --- Prepare dataset ---
        train_dataset = TimeSeriesDataset(
            windows=self._train_test_set.train_sets,
            train_window_size=self._train_test_set.train_window_size,
            validation_window_size=self._train_test_set.validation_window_size,
            test_window_size=self._train_test_set.test_window_size,
        )

        if len(train_dataset) == 0:
            raise ValueError("No valid training windows found in train_sets.")

        # --- DataLoaders ---
        train_loader = DataLoader(
            list(zip(train_dataset.X_train, train_dataset.y_train)),
            batch_size=self._model_config.batch_size,
            shuffle=False,
        )

        val_loader = DataLoader(
            list(zip(train_dataset.X_val, train_dataset.y_val)),
            batch_size=self._model_config.batch_size,
            shuffle=False,
        )

        # --- Model Setup ---
        num_features = train_dataset.X_train.shape[-1]
        model = LSTMForecastModel(
            num_features=num_features,
            hidden_size=128,
            num_layers=2,
            test_window_size=self._train_test_set.test_window_size,
        ).to(self._device)

        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(
            model.parameters(), lr=self._model_config.learning_rate
        )

        print(f"Training on {self._device} for {self._model_config.epochs} epochs...\n")

        # --- Early Stopping setup ---
        best_val_loss = float("inf")
        best_model_state = None
        epochs_no_improve = 0

        train_loss_history, val_loss_history = [], []

        for epoch in range(self._model_config.epochs):
            # ---------------- TRAIN ----------------
            model.train()
            running_loss = 0.0
            for X_batch, y_batch in tqdm(
                train_loader, desc=f"Epoch {epoch+1}", leave=False
            ):
                X_batch, y_batch = X_batch.to(self._device), y_batch.to(self._device)
                optimizer.zero_grad()
                y_pred = model(X_batch)
                loss = criterion(y_pred, y_batch)
                loss.backward()
                optimizer.step()
                running_loss += loss.item()

            avg_train_loss = running_loss / len(train_loader)
            train_loss_history.append(avg_train_loss)

            # ---------------- VALIDATION ----------------
            model.eval()
            val_running_loss = 0.0
            with torch.no_grad():
                for X_val, y_val in val_loader:
                    X_val, y_val = X_val.to(self._device), y_val.to(self._device)
                    y_val_pred = model(X_val)
                    val_loss = criterion(y_val_pred, y_val)
                    val_running_loss += val_loss.item()
            avg_val_loss = val_running_loss / len(val_loader)
            val_loss_history.append(avg_val_loss)

            # --- Early stopping check ---
            if avg_val_loss < best_val_loss:
                best_val_loss = avg_val_loss
                best_model_state = model.state_dict()
                epochs_no_improve = 0
            else:
                epochs_no_improve += 1
                if epochs_no_improve >= PATIENCE:
                    self._logger.log_info(
                        f"Early stopping triggered at epoch {epoch+1}. "
                        f"No improvement for {PATIENCE} consecutive epochs."
                    )
                    print(
                        f"Early stopping at epoch {epoch+1} (best val loss: {best_val_loss:.6f})"
                    )
                    # Restore best model state
                    model.load_state_dict(best_model_state)
                    break

            # Log & print
            log_dict = {
                "epoch": epoch + 1,
                "train_loss": avg_train_loss,
                "val_loss": avg_val_loss,
            }
            self._run.log(log_dict)
            print(
                f"Epoch [{epoch+1}/{self._model_config.epochs}] "
                f"- Train: {avg_train_loss:.8f} | Val: {avg_val_loss:.8f}"
            )

        # ---------------- TEST EVALUATION ----------------
        model.eval()
        with torch.no_grad():
            last_train_window = self._train_test_set.train_sets[-1].values
            train_window = self._train_test_set.train_window_size
            test_window = self._train_test_set.test_window_size

            X_input = (
                torch.tensor(
                    last_train_window[-train_window:, :-1], dtype=torch.float32
                )
                .unsqueeze(0)
                .to(self._device)
            )
            y_pred = model(X_input)

            test_data = self._train_test_set.test_sets[0].values
            y_true_np = np.array(test_data[:test_window, -1], dtype=np.float32)
            y_true = torch.from_numpy(y_true_np).unsqueeze(0).to(self._device)

            y_pred_denorm = (
                y_pred * (output_range[1] - output_range[0]) + output_range[0]
            )
            test_loss = criterion(y_pred_denorm, y_true).item()
            epsilon = 1e-8
            mape = (
                torch.mean(torch.abs((y_true - y_pred_denorm) / (y_true + epsilon)))
                * 100
            ).item()
            log_dict["test_mape"] = mape
            self._run.log(log_dict)

        training_time = time() - start_time
        print(f"Training completed in {training_time:.2f}s | Test MAPE: {mape:.2f}%")

        # --- Prepare model output ---
        model_output = ModelOutputDto(
            model=model,
            model_state_dict=model.state_dict(),
            model_config=self._model_config,
            train_loss_history=train_loss_history,
            final_train_loss=train_loss_history[-1],
            validation_loss_history=val_loss_history,
            final_validation_loss=best_val_loss,
            test_loss=test_loss,
            y_pred=y_pred.cpu().numpy().flatten(),
            y_pred_denorm=y_pred_denorm.cpu().numpy().flatten(),
            y_true=y_true.cpu().numpy().flatten(),
            input_window_size=train_window,
            horizon_size=test_window,
            training_time=training_time,
            mape=mape,
        )

        # --- Save outputs ---
        training_output_file = f"training_output.json"
        with open(training_output_file, "w") as f:
            json.dump(model_output.to_dict(), f, indent=4)
        self._run.save(training_output_file)

        output_pickle_file = "training_output.pkl"
        with open(output_pickle_file, "wb") as f:
            pickle.dump(model_output, f)
        self._run.save(output_pickle_file)

        self._run.finish()
        self._logger.log_info("DONE TRAINING PROCESS.\n")

        return model_output
