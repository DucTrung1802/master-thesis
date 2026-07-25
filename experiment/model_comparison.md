# Two OCR models on ACB Q1-2014 … Q4-2016

`paddle` = PaddleOCR-DB + VietOCR (experiment_8); `onnx` = DeepDoc-ONNX + VietOCR (experiment_9). Both feed the SAME statement parser, so the difference is the OCR alone. The score is figures matching the production reference by MAGNITUDE (`match / comparable`) — income-statement expense lines that agree in magnitude but flip sign are counted as matches, because the sign is a CafeF-vs-filing storage convention, not an OCR error.

## Overall

| metric | paddle | onnx |
|---|---|---|
| statements found | 36/36 | 36/36 |
| statements reconciled | 34/36 | 34/36 |
| figures match (magnitude) | 734/879 | 721/868 |
| accuracy | 0.835 | 0.8306 |
| (sign-only, not counted as error) | 61 | 60 |
| OCR pages | 144 | 168 |
| OCR seconds | 2000.7 | 237.4 |
| sec / page | 13.89 | 1.41 |

**onnx is 9.9× faster per page** (1.41 vs 13.89 s), at 0.8306 vs 0.835 accuracy.

## Per statement (match / comparable, reconciled quarters)

| statement | paddle | onnx |
|---|---|---|
| balance_sheet | 429/528 (10/12 reconcile) | 430/530 (10/12 reconcile) |
| income_statement | 207/225 (12/12 reconcile) | 200/220 (12/12 reconcile) |
| cash_flow | 98/126 (12/12 reconcile) | 91/118 (12/12 reconcile) |

## Per quarter × statement (reconcile · agree/comparable)

| period | balance·paddle | balance·onnx | income·paddle | income·onnx | cash·paddle | cash·onnx |
|---|---|---|---|---|---|---|
| Q1-2014 | OK 37/42 | OK 35/39 | OK 19/20 | OK 19/20 | OK 19/19 | OK 16/16 |
| Q2-2014 | OK 38/44 | OK 37/43 | OK 18/18 | OK 16/17 | OK 8/13 | OK 8/13 |
| Q3-2014 | OK 43/44 | OK 40/44 | OK 18/20 | OK 18/20 | OK 1/1 | OK 1/1 |
| Q4-2014 | OK 36/43 | OK 36/43 | OK 16/18 | OK 15/18 | OK 5/11 | OK 5/11 |
| Q1-2015 | REJECT 30/37 | OK 31/37 | OK 16/20 | OK 18/20 | OK 19/22 | OK 18/20 |
| Q2-2015 | OK 40/49 | OK 41/49 | OK 18/18 | OK 17/17 | OK 0/2 | OK 0/2 |
| Q3-2015 | OK 33/39 | OK 30/36 | OK 19/20 | OK 19/20 | OK 0/1 | OK 0/1 |
| Q4-2015 | OK 34/51 | OK 37/52 | OK 15/18 | OK 15/18 | OK 6/10 | OK 6/9 |
| Q1-2016 | OK 32/37 | REJECT 27/37 | OK 18/18 | OK 18/18 | OK 24/25 | OK 22/24 |
| Q2-2016 | OK 37/52 | OK 38/53 | OK 17/17 | OK 16/17 | OK 9/11 | OK 9/11 |
| Q3-2016 | REJECT 32/40 | REJECT 41/46 | OK 20/21 | OK 19/20 | OK 0/2 | OK 0/2 |
| Q4-2016 | OK 37/50 | OK 37/51 | OK 13/17 | OK 10/15 | OK 7/9 | OK 6/8 |
