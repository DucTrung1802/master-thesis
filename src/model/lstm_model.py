import json
import os
from time import time
from typing import List
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from tqdm.notebook import tqdm
import wandb
from dotenv import load_dotenv
import pickle
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
    r2_score,
)


from dtos.model_dtos.model_config_dto import ModelConfigDto
from dtos.model_dtos.model_output_dto import ModelOutputDto
from dtos.model_dtos.model_output_predict_true_dto import ModelPredictTrueWindowDto
from logger.logger import Logger
from train_test_creator.train_test_set import TrainTestSet
from utils.enums import (
    AchitectureType,
    LossFunctionType,
    MetricType,
    OptimizerType,
    ScalerType,
)
from utils.utils import set_seed

load_dotenv()


# =============================
# DATASET
# =============================
class TimeSeriesDataset(Dataset):
    """
    Each window is divided into:
        - Training segment: input_size (input)
        - Testing segment: forecast_size (forecast)
    """

    def __init__(self, windows, input_size, forecast_size):
        self.X_, self.y_ = [], []

        for window in windows:
            data = window.values
            total_needed = input_size + forecast_size
            if len(data) < total_needed:
                continue

            self.X_.append(data[:input_size, :-1])
            self.y_.append(data[input_size : input_size + forecast_size, -1])

        # Convert to tensors
        self.X_ = torch.tensor(np.array(self.X_), dtype=torch.float32)
        self.y_ = torch.tensor(np.array(self.y_), dtype=torch.float32)

    def __len__(self):
        return len(self.X_)

    def __getitem__(self, idx):
        return self.X_[idx], self.y_[idx]


# =============================
# MODEL
# =============================
class LSTMForecastModel(nn.Module):
    def __init__(self, num_features, hidden_size, num_layers, forecast_size):
        super(LSTMForecastModel, self).__init__()
        self.lstm = nn.LSTM(
            input_size=num_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=0.2,
        )
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, 64), nn.ReLU(), nn.Linear(64, forecast_size)
        )

    def forward(self, x):
        output, _ = self.lstm(x)
        last_output = output[:, -1, :]
        return self.fc(last_output)

    def get_name(self):
        method = getattr(self, "_get_name", None)
        if callable(method):
            return method()
        else:
            return "LSTMForecastModel"


# =============================
# MAIN CLASS
# =============================
class LSTM_Model:
    def __init__(
        self, logger: Logger, train_test_set: TrainTestSet, model_config: ModelConfigDto
    ):
        set_seed(model_config.seed)

        self._logger = logger

        if not self._validate_model_config_dto(model_config):
            self._logger.log_error("Invalid ModelConfigDto")
            raise ValueError("Invalid ModelConfigDto")

        self._train_test_set: TrainTestSet = train_test_set
        self._model_config: ModelConfigDto = model_config
        self._model_config.architecture = AchitectureType.LSTM
        self._device = torch.device(model_config.device)

        self._logger.log_info(f"PyTorch version: {torch.__version__}")
        print(f"PyTorch version: {torch.__version__}")

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

        if int(model_config.input_size) <= 0:
            errors.append("Input size must be greater than 0.")
        if int(model_config.forecast_size) <= 0:
            errors.append("Forecast size must be greater than 0.")

        if int(model_config.epochs) <= 0:
            errors.append("Epochs must be greater than 0.")
        if int(model_config.batch_size) <= 0:
            errors.append("Batch size must be greater than 0.")
        if not (0 < model_config.learning_rate <= 1):
            errors.append("Learning rate must be between 0 and 1.")
        if int(model_config.patience) < 0:
            errors.append("Patience must be non-negative.")

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
        start_time = time()

        # --- Prepare dataset ---
        train_dataset = TimeSeriesDataset(
            windows=self._train_test_set.train_windows,
            input_size=self._train_test_set.input_size,
            forecast_size=self._train_test_set.forecast_size,
        )

        val_dataset = TimeSeriesDataset(
            windows=self._train_test_set.val_windows,
            input_size=self._train_test_set.input_size,
            forecast_size=self._train_test_set.forecast_size,
        )

        test_dataset = TimeSeriesDataset(
            windows=self._train_test_set.test_windows,
            input_size=self._train_test_set.input_size,
            forecast_size=self._train_test_set.forecast_size,
        )

        if len(train_dataset) == 0:
            raise ValueError("No valid training windows found in train windows.")

        # --- DataLoaders ---
        train_loader = DataLoader(
            list(zip(train_dataset.X_, train_dataset.y_)),
            batch_size=self._model_config.batch_size,
            shuffle=True,
        )

        val_loader = DataLoader(
            list(zip(val_dataset.X_, val_dataset.y_)),
            batch_size=self._model_config.batch_size,
            shuffle=False,
        )

        test_loader = DataLoader(
            list(zip(test_dataset.X_, test_dataset.y_)),
            batch_size=self._model_config.batch_size,
            shuffle=False,
        )

        # --- Model Setup ---
        num_features = train_dataset.X_.shape[-1]
        model = LSTMForecastModel(
            num_features=num_features,
            hidden_size=128,
            num_layers=2,
            forecast_size=self._train_test_set.forecast_size,
        ).to(self._device)

        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(
            model.parameters(), lr=self._model_config.learning_rate
        )
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer, T_max=self._model_config.epochs
        )
        self._model_config.lr_scheduler = scheduler.__class__.__name__

        print(f"Training on {self._device} for {self._model_config.epochs} epochs...\n")

        # --- Early Stopping setup ---
        best_val_loss = float("inf")
        best_model_state = None
        epochs_no_improve = 0

        train_loss_history, val_loss_history = [], []

        # Initialize WandB
        wandb.login(key=os.getenv("WANDB_KEY"))
        self._run = wandb.init(
            entity=self._model_config.entity,
            project=self._model_config.project,
            config=self._model_config.to_dict(),
        )

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

            # ---- Scheduler Step (after validation) ----
            scheduler.step()
            current_lr = scheduler.get_last_lr()[0]

            # --- Early stopping check ---
            if avg_val_loss < best_val_loss:
                best_val_loss = avg_val_loss
                best_model_state = model.state_dict()
                epochs_no_improve = 0
            else:
                epochs_no_improve += 1
                if epochs_no_improve >= self._model_config.patience:
                    self._logger.log_info(
                        f"Early stopping triggered at epoch {epoch+1}. "
                        f"No improvement for {self._model_config.patience} consecutive epochs."
                    )
                    print(
                        f"Early stopping at epoch {epoch+1} "
                        f"(best val loss: {best_val_loss:.6f})"
                    )
                    model.load_state_dict(best_model_state)
                    break

            # ---- Logging ----
            log_dict = {
                "epoch": epoch + 1,
                "train_loss": avg_train_loss,
                "val_loss": avg_val_loss,
                "learning_rate": current_lr,
            }
            self._run.log(log_dict)

            print(
                f"Epoch [{epoch+1}/{self._model_config.epochs}] "
                f"- Train: {avg_train_loss:.8f} | Val: {avg_val_loss:.8f} | LR: {current_lr:.6e}"
            )

        # ---------------- TEST EVALUATION ----------------
        model.eval()
        test_running_loss = 0.0

        all_y_true = []
        all_y_pred = []

        with torch.no_grad():
            for X_test, y_test in test_loader:
                X_test = X_test.to(self._device)
                y_test = y_test.to(self._device)

                # Forward pass (normalized)
                y_pred_norm = model(X_test)

                # Inverse transform predictions
                y_pred_denorm = self._train_test_set.target_scaler.inverse_transform(
                    y_pred_norm.cpu().numpy()
                )
                y_pred_denorm = torch.tensor(y_pred_denorm, device=self._device)

                # Inverse transform ground truth
                y_test_denorm = self._train_test_set.target_scaler.inverse_transform(
                    y_test.cpu().numpy()
                )
                y_test_denorm = torch.tensor(y_test_denorm, device=self._device)

                # Loss on real scale
                loss = criterion(y_pred_denorm, y_test_denorm)
                test_running_loss += loss.item()

                # Store outputs
                all_y_true.append(y_test_denorm.detach().cpu())
                all_y_pred.append(y_pred_denorm.detach().cpu())

        # Average test loss
        test_loss = test_running_loss / len(test_loader)

        # Concatenate all windows
        y_true_np = torch.cat(all_y_true, dim=0).numpy().reshape(-1)
        y_pred_np = torch.cat(all_y_pred, dim=0).numpy().reshape(-1)

        # Metrics
        mae = mean_absolute_error(y_true_np, y_pred_np)
        mape = mean_absolute_percentage_error(y_true_np, y_pred_np) * 100
        rmse = root_mean_squared_error(y_true_np, y_pred_np)
        r2 = r2_score(y_true_np, y_pred_np)

        # Log test metrics
        self._run.log(
            {
                "test_loss": test_loss,
                "test_mae": mae,
                "test_mape": mape,
                "test_rmse": rmse,
                "test_r2": r2,
            }
        )

        training_time = time() - start_time

        print(f"Training completed in {training_time:.2f}s")
        print(f"Test MAE: {mae:.6f}")
        print(f"Test MAPE: {mape:.2f}%")
        print(f"Test RMSE: {rmse:.6f}")
        print(f"Test R2: {r2:.6f}\n")

        # ---------------- CREATE PREDICTIONS ----------------
        output_windows = []
        for i in range(
            0, len(self._train_test_set.test_set), self._train_test_set.forecast_size
        ):
            total_window_size = (
                self._train_test_set.input_size + self._train_test_set.forecast_size
            )
            window_df = self._train_test_set.test_set.iloc[
                i : i
                + self._train_test_set.input_size
                + self._train_test_set.forecast_size
            ].reset_index(drop=True)

            if len(window_df) == total_window_size:
                output_windows.append(window_df)

        output_dataset = TimeSeriesDataset(
            windows=output_windows,
            input_size=self._train_test_set.input_size,
            forecast_size=self._train_test_set.forecast_size,
        )

        output_loader = DataLoader(
            list(zip(output_dataset.X_, output_dataset.y_)),
            batch_size=self._model_config.batch_size,
            shuffle=False,
        )

        model_output_predict_true_dtos: List[ModelPredictTrueWindowDto] = []
        idx = 0
        with torch.no_grad():
            for X_test, y_test in output_loader:
                X_test = X_test.to(self._device)
                y_test = y_test.to(self._device)

                # Forward pass (normalized)
                y_pred_norm = model(X_test)

                # Inverse transform predictions
                y_pred_denorm = self._train_test_set.target_scaler.inverse_transform(
                    y_pred_norm.cpu().numpy()
                )

                y_pred_denorm = torch.tensor(y_pred_denorm, device=self._device)

                # Inverse transform ground truth
                y_test_denorm = self._train_test_set.target_scaler.inverse_transform(
                    y_test.cpu().numpy()
                )
                y_test_denorm = torch.tensor(y_test_denorm, device=self._device)

                # Create data for ModelPredictTrueWindowDto
                history_data = self._train_test_set.train_set.iloc[
                    self._train_test_set.forecast_size
                    * idx : self._train_test_set.forecast_size
                    * idx
                    + self._train_test_set.input_size,
                    -1,
                ].tolist()

                model_output_predict_true_dto = ModelPredictTrueWindowDto(
                    index=idx,
                    history_data=history_data,
                    y_pred_denorm=y_pred_denorm.detach()
                    .cpu()
                    .numpy()
                    .reshape(-1)
                    .tolist(),
                    y_true=y_test_denorm.detach().cpu().numpy().reshape(-1).tolist(),
                )
                model_output_predict_true_dtos.append(model_output_predict_true_dto)

                idx += 1

        # ---------------- MODEL OUTPUT ----------------
        model_output = ModelOutputDto(
            # Dataset metadata
            dataset_name=self._train_test_set.get_name(),
            data_set_size=len(self._train_test_set.get_data_set()),
            train_set_size=len(self._train_test_set.get_train_set()),
            val_set_size=len(self._train_test_set.get_val_set()),
            test_set_size=len(self._train_test_set.get_test_set()),
            number_of_train_window=len(self._train_test_set.get_train_windows()),
            number_of_val_window=len(self._train_test_set.get_val_windows()),
            number_of_test_window=len(self._train_test_set.get_test_windows()),
            input_size=self._train_test_set.input_size,
            forecast_size=self._train_test_set.forecast_size,
            # Model metadata
            model_name=model.get_name(),
            model_config=self._model_config,
            training_time=training_time,
            # Training history
            train_loss_history=train_loss_history,
            validation_loss_history=val_loss_history,
            final_train_loss=train_loss_history[-1],
            final_validation_loss=best_val_loss,
            test_loss=test_loss,
            # Result
            model_output_predict_true_dtos=model_output_predict_true_dtos,
            # Metrics
            mae=mae,
            mape=mape,
            rmse=rmse,
            r2=r2,
        )

        # ---------------- SAVE OUTPUTS ----------------
        training_output_file = "training_output.json"
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
