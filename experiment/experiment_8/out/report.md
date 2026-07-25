# experiment_8 — paddleocr-db + vietocr-vgg_transformer

- file: `FY-2013_bao_cao_tai_chinh_hop_nhat_nam_2013_da_kiem_toan.pdf`
- pages read: 15 (0 from cache)
- OCR: 15 pages in 234.0 s (16 s/page)
- wall clock: 234.4 s

| statement | pages | rows | mapped | reconciles | agree | differ | missed |
|---|---|---|---|---|---|---|---|
| balance_sheet | [8, 9, 10] | 63 | 43 | yes | 22 | 18 | 31 |
| income_statement | [11, 12] | 25 | 19 | yes | 7 | 10 | 0 |
| cash_flow | [13, 14] | 38 | 11 | yes | 6 | 4 | 26 |

`agree` / `differ` are canonical columns both this parse and CafeF's own transcription populated; `missed` are lines CafeF has and the OCR did not recover.
