# 📌 How to Split Train – Validation – Test

### Branch: `method_2`


### Input size: `240`
### Forecast horizon: `30`
### Example dataset size: `3000`


## Overall Structure

`|<------------------------------ 3000 data points ------------------------------->|`

`[-------------------- Train: 2820 --------------------]`

`[----- Validation: 90 -----]`

`[----- Test: 90 -----]`



- **Test set:** last **90** points
- **Validation set**: **90** points before Test set
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


## Validation Windows (sliding)

Validation uses **60** windows:

- `X_val[0]  = train_block[-240 : ]`
- `Y_val[0]  = validation_block[0 : 30]`

- ...

- `X_val[59]  = train_block[-180 : ] + validation_block[0 : 60]`
- `Y_val[59]  = validation_block[60 : 90]`

Validation is evaluated by averaging metrics across sliding windows.

## Test Windows (sliding)

Test uses **60** windows:

- `X_test[0] = train_block[-150 : ] + validation_block[0 : 90]`
- `Y_test = test_block[0 : 30]`

- ...

- `X_test[59] = train_block[-90 : ] + validation_block[0 : 90] + test_block[0 : 60]`
- `Y_test[59] = test_block[60 : 90]`

Test is evaluated by averaging metrics, with sliding, to simulate true future forecasting.


## Metric for evaluation

Training loss: MSE  
Evaluation metrics: MAE, MAPE, RMSE, R^2


## ✅ Summary

### **Train set:**  
- Uses many sliding windows (240 → 30) with stride 1.

### **Validation set:**  
- Consists of **60 rolling-origin windows** over a 90-point block.
- Inputs always use the most recent 240 observed points.
- Outputs are the next 30 future points.

### **Test set:**  
- Same rolling-origin strategy as validation (60 windows).
- Used only once for final model evaluation (not for tuning or training).


