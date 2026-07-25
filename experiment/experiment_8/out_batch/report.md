# paddle — ACB batch

- statements reconciled: **34/36**
- figures matching the production reference (magnitude): **734/879** (0.835) — of which 61 agree in magnitude but flip sign (income-statement expense convention, not an OCR error)
- OCR: 144 pages in 2000.7 s (13.89 s/page)

## Per statement

| statement | found | reconciled | match | (of which sign-only) | differ | missed |
|---|---|---|---|---|---|---|
| balance_sheet | 12/12 | 10 | 429 | 0 | 99 | 335 |
| income_statement | 12/12 | 12 | 207 | 61 | 18 | 55 |
| cash_flow | 12/12 | 12 | 98 | 0 | 28 | 141 |

## Per quarter

| period | balance_sheet | income_statement | cash_flow |
|---|---|---|---|
| Q1-2014 | OK · 37/42 | OK · 19/20 | OK · 19/19 |
| Q1-2015 | REJECT · 30/37 | OK · 16/20 | OK · 19/22 |
| Q1-2016 | OK · 32/37 | OK · 18/18 | OK · 24/25 |
| Q2-2014 | OK · 38/44 | OK · 18/18 | OK · 8/13 |
| Q2-2015 | OK · 40/49 | OK · 18/18 | OK · 0/2 |
| Q2-2016 | OK · 37/52 | OK · 17/17 | OK · 9/11 |
| Q3-2014 | OK · 43/44 | OK · 18/20 | OK · 1/1 |
| Q3-2015 | OK · 33/39 | OK · 19/20 | OK · 0/1 |
| Q3-2016 | REJECT · 32/40 | OK · 20/21 | OK · 0/2 |
| Q4-2014 | OK · 36/43 | OK · 16/18 | OK · 5/11 |
| Q4-2015 | OK · 34/51 | OK · 15/18 | OK · 6/10 |
| Q4-2016 | OK · 37/50 | OK · 13/17 | OK · 7/9 |
