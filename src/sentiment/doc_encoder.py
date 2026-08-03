"""Whole-document PhoBERT encoding by chunking — because 256 tokens is not a document.

## Why this exists

`text_reaction_model.embed_texts` truncates at 256 tokens, and `build_text` feeds it the
headline plus the first 600 characters. Measured on this corpus (4,000-article sample,
`vinai/phobert-base` tokenizer):

| | tokens |
|---|---|
| headline + FULL content | mean **749**, median 573, p95 1,913, max 5,325 |
| headline + first 600 chars | mean **177** |

**So the old representation saw 38.7% of the average article and threw the rest away.**
PhoBERT-base cannot be given more — `max_position_embeddings` is 258 — so reading a whole
document means chunking it and pooling, which is also what paper 51 does (sentence-level
scores averaged into one document score).

## The representation

Each article → windows of `CHUNK_TOKENS`, each encoded independently (frozen, mean-pooled
over the attention mask), then combined:

    lead  = the FIRST chunk only                         → 768   (≈ the old representation)
    full  = concat[ mean(chunks), max(chunks), lead ]    → 2304

Both are returned, because **the ablation is the finding**: if `full` and `lead` score the
same, the extra 61% of text carries nothing, and that is worth knowing before anyone
proposes a long-context model.

`max` is carried alongside `mean` on purpose — a 20-chunk article averages a single
decisive paragraph into noise, and max-pooling is the cheapest way to keep a strong local
signal. Together they are the standard hierarchical-pooling pair.

⚠️ Frozen, not fine-tuned. Fine-tuning PhoBERT is a separate question with a separate
answer (see `experiment/experiment_10/news_result.py` §feasibility).
"""

from __future__ import annotations

import os

# ⚠️ Same reason as sentiment_functions: `from_pretrained` does a network HEAD request on
# every load and hangs for minutes behind a blocked connection. Weights are cached locally.
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

from typing import Dict, List, Sequence

import numpy as np

ENCODER_NAME = "vinai/phobert-base"

#: PhoBERT's positional table is 258 = 256 + <s> + </s>, so a window is 254 content tokens.
CHUNK_TOKENS = 254

#: Chunks per article. At 254 tokens and no overlap the corpus averages 3.44 chunks and
#: p95 is 8; 16 covers 100.0% of the text while bounding the 5,325-token tail.
MAX_CHUNKS = 16

EMB_DIM = 768
DOC_DIM = EMB_DIM * 3  # mean ‖ max ‖ lead


def build_document_text(headline, content) -> List[str]:
    """headline + the WHOLE body. No slicing — that is the point of this module."""
    h = headline.fillna("").astype(str).str.strip()
    c = content.fillna("").astype(str).str.strip()
    return (h + ". " + c).tolist()


def encode_documents(
    texts: Sequence[str],
    batch_chunks: int = 96,
    max_chunks: int = MAX_CHUNKS,
    chunk_tokens: int = CHUNK_TOKENS,
    log_every: int = 400,
) -> Dict[str, np.ndarray]:
    """→ `{"lead": (n, 768), "full": (n, 2304)}`.

    Chunks from ALL articles are flattened into one stream and batched together, so a
    2-chunk article and a 16-chunk article cost the same per chunk. Encoding runs in fp16
    on CUDA — the 4 GB card cannot hold a comfortable fp32 batch at this width, and the
    encoder is frozen so reduced precision costs nothing that matters.
    """
    import torch
    from transformers import AutoModel, AutoTokenizer

    tok = AutoTokenizer.from_pretrained(ENCODER_NAME)
    model = AutoModel.from_pretrained(ENCODER_NAME)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device).eval()
    if device == "cuda":
        model.half()

    bos, eos, pad = tok.bos_token_id, tok.eos_token_id, tok.pad_token_id

    # ── flatten every article into (article index, token window) ─────────────────────
    owner: List[int] = []
    windows: List[List[int]] = []
    for i, text in enumerate(texts):
        ids = tok(text, add_special_tokens=False, truncation=False)["input_ids"]
        if not ids:
            ids = [eos]
        for c in range(0, min(len(ids), max_chunks * chunk_tokens), chunk_tokens):
            windows.append(ids[c : c + chunk_tokens])
            owner.append(i)

    owner_arr = np.asarray(owner)
    n_docs, n_chunks = len(texts), len(windows)
    print(f"    {n_docs:,} documents → {n_chunks:,} chunks "
          f"({n_chunks / max(n_docs, 1):.2f}/doc), fp16 on {device}")

    vecs = np.empty((n_chunks, EMB_DIM), dtype=np.float32)
    for start in range(0, n_chunks, batch_chunks):
        batch = windows[start : start + batch_chunks]
        width = max(len(w) for w in batch) + 2
        ids = np.full((len(batch), width), pad, dtype=np.int64)
        mask = np.zeros((len(batch), width), dtype=np.int64)
        for r, w in enumerate(batch):
            row = [bos] + w + [eos]
            ids[r, : len(row)] = row
            mask[r, : len(row)] = 1

        t_ids = torch.from_numpy(ids).to(device)
        t_mask = torch.from_numpy(mask).to(device)
        with torch.no_grad():
            out = model(input_ids=t_ids, attention_mask=t_mask).last_hidden_state
            m = t_mask.unsqueeze(-1).to(out.dtype)
            pooled = (out * m).sum(1) / m.sum(1).clamp(min=1)
        vecs[start : start + len(batch)] = pooled.float().cpu().numpy()

        if log_every and (start // batch_chunks) % log_every == 0 and start:
            print(f"      {start:,}/{n_chunks:,} chunks", flush=True)

    # ── pool chunks back into one vector per document ────────────────────────────────
    lead = np.zeros((n_docs, EMB_DIM), dtype=np.float32)
    doc = np.zeros((n_docs, DOC_DIM), dtype=np.float32)
    order = np.argsort(owner_arr, kind="stable")
    owner_sorted = owner_arr[order]
    bounds = np.searchsorted(owner_sorted, np.arange(n_docs + 1))
    for i in range(n_docs):
        sl = order[bounds[i] : bounds[i + 1]]
        if len(sl) == 0:
            continue
        block = vecs[sl]
        lead[i] = block[0]
        doc[i] = np.concatenate([block.mean(0), block.max(0), block[0]])

    return {"lead": lead, "full": doc}
