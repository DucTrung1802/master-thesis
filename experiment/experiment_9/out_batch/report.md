# onnx — ACB batch

- statements reconciled: **34/36**
- figures matching the production reference (magnitude): **721/868** (0.8306) — of which 60 agree in magnitude but flip sign (income-statement expense convention, not an OCR error)
- OCR: 168 pages in 237.4 s (1.41 s/page)

## Per statement

| statement | found | reconciled | match | (of which sign-only) | differ | missed |
|---|---|---|---|---|---|---|
| balance_sheet | 12/12 | 10 | 430 | 0 | 100 | 333 |
| income_statement | 12/12 | 12 | 200 | 60 | 20 | 60 |
| cash_flow | 12/12 | 12 | 91 | 0 | 27 | 149 |

## Per quarter

| period | balance_sheet | income_statement | cash_flow |
|---|---|---|---|
| Q1-2014 | OK · 35/39 | OK · 19/20 | OK · 16/16 |
| Q1-2015 | OK · 31/37 | OK · 18/20 | OK · 18/20 |
| Q1-2016 | REJECT · 27/37 | OK · 18/18 | OK · 22/24 |
| Q2-2014 | OK · 37/43 | OK · 16/17 | OK · 8/13 |
| Q2-2015 | OK · 41/49 | OK · 17/17 | OK · 0/2 |
| Q2-2016 | OK · 38/53 | OK · 16/17 | OK · 9/11 |
| Q3-2014 | OK · 40/44 | OK · 18/20 | OK · 1/1 |
| Q3-2015 | OK · 30/36 | OK · 19/20 | OK · 0/1 |
| Q3-2016 | REJECT · 41/46 | OK · 19/20 | OK · 0/2 |
| Q4-2014 | OK · 36/43 | OK · 15/18 | OK · 5/11 |
| Q4-2015 | OK · 37/52 | OK · 15/18 | OK · 6/9 |
| Q4-2016 | OK · 37/51 | OK · 10/15 | OK · 6/8 |
