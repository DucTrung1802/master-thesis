import pandas as pd
import pmdarima as pm
from statsmodels.tsa.arima.model import ARIMA
import warnings
import matplotlib.pyplot as plt

warnings.filterwarnings(
    "ignore", category=FutureWarning, message=".*force_all_finite.*"
)

# ===============================
# Load dataset
# ===============================
df = pd.read_csv("sample_data/example_time_series_data.csv")
df["ds"] = pd.to_datetime(df["ds"])

# Train-test split
train = df[df["ds"] < "2015-01-01"]
test = df[df["ds"] >= "2015-01-01"]

# ===============================
# Auto-ARIMA for best parameters
# ===============================
# model = pm.auto_arima(
#     train["y"],
#     start_p=1,
#     start_q=1,
#     max_p=10,
#     max_q=10,
#     m=1,
#     d=1,
#     seasonal=True,
#     start_P=1,
#     start_Q=1,
#     D=0,
#     trace=True,
#     error_action="ignore",
#     suppress_warnings=True,
#     stepwise=True,
# )

# best_order = model.order
# print(f"Best ARIMA order: {best_order}") # (7, 1, 9)

best_order = (7, 1, 9)


# ===============================
# Rolling Forecast Function
# ===============================
def rolling_forecast(df, test, best_order, mode="expanding", window_size=None):
    """
    Faster rolling forecast using ARIMA.append to avoid full refits.
    mode: "expanding" (default) or "sliding"
    window_size: required if mode="sliding" (in days)
    """

    # Precompute for speed
    df = df.copy().sort_values("ds")
    test = test.copy().sort_values("ds")
    test["ds_pet"] = test["ds"] - pd.Timedelta(days=7)

    df_preds = []

    # Initial training set (first forecast point)
    first_pet = test.iloc[0]["ds_pet"]

    if mode == "expanding":
        train_init = df[df["ds"] < first_pet]
    elif mode == "sliding":
        if window_size is None:
            raise ValueError("window_size must be set for sliding mode")
        train_init = df[
            (df["ds"] < first_pet)
            & (df["ds"] >= first_pet - pd.Timedelta(days=window_size))
        ]
    else:
        raise ValueError("mode must be 'expanding' or 'sliding'")

    # Fit initial ARIMA once
    model_refit = ARIMA(train_init["y"], order=best_order).fit()

    # Loop through test set
    for _, row in test.iterrows():
        start_date = row["ds"]
        pet = row["ds_pet"]

        # Get actual y
        test_rolling = df[df["ds"] == start_date]
        if test_rolling.empty:
            continue

        # Forecast 1 step
        fc_test = model_refit.forecast(1).iloc[0]

        df_preds.append(
            {
                "ds": start_date,
                "actual_y": test_rolling.iloc[0]["y"],
                "test_pred": fc_test,
            }
        )

        # Update model with actual new observation
        model_refit = model_refit.append([test_rolling.iloc[0]["y"]], refit=False)

    return pd.DataFrame(df_preds)


# ===============================
# Run Both Modes
# ===============================

# Expanding window forecast
df_preds_expanding = rolling_forecast(df, test, best_order, mode="expanding")

# Sliding window forecast (e.g., last 365 days history only)
df_preds_sliding = rolling_forecast(
    df, test, best_order, mode="sliding", window_size=365
)


# ===============================
# Plot Forecast vs Test Data (2 Subplots)
# ===============================
fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

# --- Expanding Window ---
axes[0].plot(test["ds"], test["y"], label="Test (Actual)", color="green")
axes[0].plot(
    df_preds_expanding["ds"],
    df_preds_expanding["test_pred"],
    label="Forecast (Expanding)",
    color="red",
    linestyle="--",
)
axes[0].set_title("ARIMA Forecast vs Actual (Expanding Window)")
axes[0].set_ylabel("Value")
axes[0].legend()
axes[0].grid(True, linestyle="--", alpha=0.7)

# --- Sliding Window ---
axes[1].plot(test["ds"], test["y"], label="Test (Actual)", color="green")
axes[1].plot(
    df_preds_sliding["ds"],
    df_preds_sliding["test_pred"],
    label="Forecast (Sliding)",
    color="blue",
    linestyle="--",
)
axes[1].set_title("ARIMA Forecast vs Actual (Sliding Window)")
axes[1].set_xlabel("Date")
axes[1].set_ylabel("Value")
axes[1].legend()
axes[1].grid(True, linestyle="--", alpha=0.7)

plt.tight_layout()
plt.show()
