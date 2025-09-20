import pandas as pd
import pmdarima as pm  # to detect ARIMA parameters using auto-arima
from statsmodels.tsa.arima.model import ARIMA  # to build the ARIMA model
import warnings
import matplotlib.pyplot as plt

warnings.filterwarnings(
    "ignore", category=FutureWarning, message=".*force_all_finite.*"
)

df = pd.read_csv("sample_data/example_time_series_data.csv")


# Prepare the data for ARIMA
train = df[df["ds"] < "2015-01-01"]  # 7 years of data for training
test = df[df["ds"] >= "2015-01-01"]  # ~1 year and 1 month of data for test


# Auto-ARIMA to detect ARIMA model parameters
model = pm.auto_arima(
    train.y,
    start_p=1,
    start_q=1,
    max_p=10,
    max_q=10,
    m=1,  # frequency of series set to annual
    d=1,  # 'd' determined manually using the adf test
    seasonal=True,
    start_P=1,
    start_Q=1,
    D=0,
    trace=True,
    error_action="ignore",
    suppress_warnings=True,
    stepwise=True,
)


# Extracting the ARIMA parameters
best_order = model.get_params().get(
    "order"
)  # order (7,1,9) is obtained from `get_params()` method

print(f"Best order: {best_order}")

# Build the ARIMA model
model_fit = ARIMA(
    train.y, order=best_order
)  # order for AR, I, MA components were obtained from auto-ARIMA
fitted = model_fit.fit()


# Forecast for the test period of 2015 (383 data points)
fc = fitted.forecast(383)


# Plot
plt.figure(figsize=(14, 6))
plt.plot(train["ds"], train["y"], label="Train", color="blue")
plt.plot(test["ds"], test["y"], label="Test", color="green")
plt.plot(test["ds"], fc, label="Forecast", color="red", linestyle="--")

plt.xlabel("Date")
plt.ylabel("Value")
plt.title("ARIMA Forecast vs Actuals")
plt.legend()
plt.show()
