# experiment_9 — deepdoc-det-onnx + vietocr-vgg_seq2seq

- file: `FY-2013_bao_cao_tai_chinh_hop_nhat_nam_2013_da_kiem_toan.pdf`
- pages read: 15 (0 from cache)
- OCR: 15 pages in 27.3 s (2 s/page)
- wall clock: 27.7 s

| statement | pages | rows | mapped | reconciles | agree | differ | missed |
|---|---|---|---|---|---|---|---|
| balance_sheet | [8, 9, 10] | 62 | 44 | yes | 21 | 20 | 30 |
| income_statement | [11, 12] | 25 | 19 | yes | 7 | 9 | 1 |
| cash_flow | [13, 14] | 38 | 16 | yes | 11 | 3 | 22 |

`agree` / `differ` are canonical columns both this parse and CafeF's own transcription populated; `missed` are lines CafeF has and the OCR did not recover.
