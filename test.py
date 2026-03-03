def train(self, validation_window_size: int = 30):
    self._logger.log_info("START TRAINING PROCESS...")
    output_range = self._train_test_set.output_range

    start_time = time()

    # --- Prepare dataset ---
    train_dataset = TimeSeriesDataset(
        windows=self._train_test_set.train_sets,
        train_window_size=self._train_test_set.train_window_size,
        validation_window_size=validation_window_size,
        test_window_size=self._train_test_set.test_window_size,
    )

    if len(train_dataset) == 0:
        raise ValueError("No valid training windows found in train_sets.")

    # --- DataLoaders ---
    train_loader = DataLoader(
        list(zip(train_dataset.X_train, train_dataset.y_train)),
        batch_size=self._model_config.batch_size,
        shuffle=True,
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

    # --- Training loop ---
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

        # Log & print
        log_dict = {
            "epoch": epoch + 1,
            "train_loss": avg_train_loss,
            "val_loss": avg_val_loss,
        }
        self._run.log(log_dict)
        print(
            f"Epoch [{epoch+1}/{self._model_config.epochs}] "
            f"- Train: {avg_train_loss:.6f} | Val: {avg_val_loss:.6f}"
        )

    # ---------------- TEST EVALUATION ----------------
    model.eval()
    with torch.no_grad():
        last_train_window = self._train_test_set.train_sets[-1].values
        train_window = self._train_test_set.train_window_size
        test_window = self._train_test_set.test_window_size

        X_input = (
            torch.tensor(last_train_window[-train_window:, :-1], dtype=torch.float32)
            .unsqueeze(0)
            .to(self._device)
        )
        y_pred = model(X_input)

        test_data = self._train_test_set.test_sets[0].values
        y_true_np = np.array(test_data[:test_window, -1], dtype=np.float32)
        y_true = torch.from_numpy(y_true_np).unsqueeze(0).to(self._device)

        y_pred_denorm = y_pred * (output_range[1] - output_range[0]) + output_range[0]
        test_loss = criterion(y_pred_denorm, y_true).item()
        epsilon = 1e-8
        mape = (
            torch.mean(torch.abs((y_true - y_pred_denorm) / (y_true + epsilon))) * 100
        ).item()

    training_time = time() - start_time
    print(f"Training completed in {training_time:.2f}s | Test MAPE: {mape:.2f}%")

    # --- Prepare model output ---
    model_output = ModelOutputDto(
        model=model,
        model_state_dict=model.state_dict(),
        model_config=self._model_config,
        train_loss_history=train_loss_history,
        final_train_loss=train_loss_history[-1],
        test_loss=test_loss,
        y_pred=y_pred.cpu().numpy().flatten(),
        y_pred_denorm=y_pred_denorm.cpu().numpy().flatten(),
        y_true=y_true.cpu().numpy().flatten(),
        input_window_size=train_window,
        horizon_size=test_window,
        training_time=training_time,
    )

    model_output.validation_loss_history = val_loss_history
    model_output.mape = mape

    self._logger.log_info("DONE TRAINING PROCESS.\n")
    return model_output
