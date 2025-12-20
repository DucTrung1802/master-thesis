# 📌 How to Split Train – Validation – Test

### Branch: `method_1`


### Input size: `240`
### Forecast horizon: `30`
### Example dataset size: `3000`


## Overall Structure

`|<------------------------------ 3000 data points ------------------------------->|`

`[-------------------- Train (~2460 points) --------------------]`

`[----- Validation: 240 + 30 -----]`

`[----- Test: 240 + 30 -----]`



- **Test set:** last **270** points = **240 input days + 30 forecast days**
- **Validation set:** previous **270** points = **240 input days + 30 forecast days**
- **Train set:** all earlier points


## Loss function: `MSE`


## How to Create Training Windows

Training uses sliding windows across the Train block:

1. **Each training sample (window):**
   - Input: 240 days
   - Output: 30 days

2. **Sliding step:** 1

3. **Generation:**
   - For `i = 0` → `train_size - 240 - 30`:
     - `X_train[i] = data[i : i + 240]`
     - `Y_train[i] = data[i + 240 : i + 240 + 30]`


## Validation Windows (not sliding)

Validation uses **only one window**:

- `X_val  = validation_block[0 : 240]`
- `Y_val  = validation_block[240 : 270]`

This matches real forecasting:  
240 days of recent history → forecast next 30 days.


## Test Windows (not sliding)

Test uses **only one final window**:

- `X_test = test_block[0 : 240]`
- `Y_test = test_block[240 : 270]`

Test must be evaluated **once**, without sliding, to simulate true future forecasting.


## Metric for evaluation

- MAE
- MAPE
- RMSE
- R2


## Summary

- Train uses **many** sliding windows (240→30).
- Validation and Test each use **one** window:
  - Input = **the first 240 days** of the block  
  - Output = **the last 30 days** of the block
- Ensures correct chronological order and no data leakage.
