# src\web_scraper\cafef_schema.py

# ===== Standard Library =====
import json
import re
import unicodedata
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
REFERER = "https://cafef.vn/du-lieu/"
API = "https://apiweb.cafef.vn/api/{v}/BCTC/{ep}"

BALANCE_SHEET = "balance_sheet"
INCOME_STATEMENT = "income_statement"
CASH_FLOW = "cash_flow"
REPORTS = (BALANCE_SHEET, INCOME_STATEMENT, CASH_FLOW)

# There is no single Vietnamese chart of accounts — there is one per ACCOUNTING TEMPLATE, and
# a schema is only valid within its own. Four exist among listed companies:
#
#   bank (TCTD)       credit institutions. Income statement opens at interest income.
#   corp (DN)         everyone else — 9 of the 10 sectors. Opens at revenue.
#   securities (CTCK) brokers. Opens at "I. DOANH THU HOẠT ĐỘNG"; an 81-line P&L.
#   insurance (DNBH)  insurers. Opens at "Doanh thu phí bảo hiểm".
#
# They share no line items, so their columns must never be mixed in one table: a bank and a
# retailer both have a "code 1", and it means interest income for one and revenue for the
# other. Schema files are named `schema_<template>_<report>.csv`.
BANK = "bank"
CORP = "corp"
SECURITIES = "securities"
INSURANCE = "insurance"
TEMPLATES = (BANK, CORP, SECURITIES, INSURANCE)

# Any ticker on the template will do — the chart of accounts is a property of the template,
# not of the company. That is the whole point of pinning it down: a column name then means the
# same line for every ticker filing on it.
REFERENCE = {BANK: "VCB", CORP: "FPT", SECURITIES: "SSI", INSURANCE: "BVH"}

# A template's FINGERPRINT: the number of line items CafeF renders in each section, in the
# order (BS-assets, BS-equity, P&L, CF-operating, CF-investing, CF-financing).
#
# The cash-flow OPERATING count varies because a company CHOOSES its method — indirect (opens
# at "Lợi nhuận trước thuế") or direct (opens at "Tiền thu từ bán hàng") — so a template has
# one fingerprint per method it is seen with. The choice is NOT a property of the sector or of
# the template: ANV and DIG file direct while their sector-mates FPT/VNM/VIC file indirect,
# and BLI files direct where BVH files indirect.
FINGERPRINTS = {
    (47, 43, 26, 26, 10, 11): BANK,
    (79, 54, 24, 19, 8, 12): CORP,          # indirect
    (79, 54, 24, 8, 8, 12): CORP,           # direct
    (67, 65, 81, 45, 6, 21): SECURITIES,
    (68, 27, 54, 19, 8, 10): INSURANCE,     # indirect
    (68, 27, 54, 8, 8, 11): INSURANCE,      # direct
}

# What the operating section opens with tells you the method — the only test needed.
#
# The rule is defined on the INDIRECT side, and must be: indirect IS "start from profit before
# tax and adjust", so it always opens on that line. Direct is "receipts and payments", and its
# opening line is worded differently by every template and half the tickers — VCB "Thu nhập lãi
# … nhận được", ANV "Tiền thu TỪ bán hàng", BLI "Tiền thu bán hàng" (no "từ"). Enumerating
# those is a losing game; anything that is not indirect is direct.
INDIRECT_OPENS = "loi nhuan truoc thue"
INDIRECT = "indirect"
DIRECT = "direct"

# The three tabs of cafef.vn/du-lieu/<exchange>/<sym>-tai-chinh.chn. Each is backed by one
# JSON endpoint whose `templace` block IS the chart of accounts — the line items, in
# statement order, with their numbering carried inside the names.
TABS = {
    #                     url                                    sections           shape
    BALANCE_SHEET:    (API.format(v="v2", ep="GetReportCDKT"),   ("TN", "NV"),     "nested"),
    INCOME_STATEMENT: (API.format(v="v1", ep="GetReportDetail"), ("KQKD",),        "flat"),
    CASH_FLOW:        (API.format(v="v1", ep="GetReportLCTT"),   ("HDKD", "HDDT",
                                                                  "HDTC"),         "nested"),
}

ROMAN = {"i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6, "vii": 7, "viii": 8,
         "ix": 9, "x": 10, "xi": 11, "xii": 12, "xiii": 13, "xiv": 14, "xv": 15,
         "xvi": 16, "xvii": 17, "xviii": 18, "xix": 19, "xx": 20}

# How much of the account name to keep. The index prefix ("hdkd_", "xii_5_") sits in front of
# it, so a column runs a few characters longer than this.
#
# 120 is not arbitrary: the longest line CafeF prints is a cash-flow item of 113 characters
# ("Tiền thu từ phát hành giấy tờ có giá dài hạn có đủ điều kiện tính vào vốn tự có…"), and
# nothing may be clipped — that line and the one below it ("Tiền chi thanh toán giấy tờ có
# giá dài hạn…") agree for their first 100 characters, so a shorter cap would collapse two
# different lines onto one column name.
MAX_ACCOUNT = 120


@dataclass
class Item:
    """One line of the canonical chart of accounts."""
    column: str          # vii_1_mua_no
    code: str            # CafeF's own code, kept for provenance ("" for header rows)
    name: str            # as CafeF prints it, e.g. "1. Mua nợ"
    account: str         # the name with its numbering stripped, e.g. "Mua nợ"
    section: str         # TN | NV | KQKD | HDKD | HDDT | HDTC
    level: int           # 1 = roman, 2 = digit, 3 = letter, 0 = total / header
    order: int           # position in the statement
    method: str = ""     # cash-flow operating lines only: indirect | direct


def _norm(s: str) -> str:
    s = (s or "").replace("đ", "d").replace("Đ", "D")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^A-Za-z0-9]+", " ", s).strip().lower()
    return re.sub(r"\s+", " ", s)


def slug(s: str, maxlen: int = MAX_ACCOUNT) -> str:
    return re.sub(r"\s+", "_", _norm(s))[:maxlen].strip("_")


# The numbering CafeF prints in front of a name. It is inconsistent — "VIII ." has a space
# before the dot, "1 Tài sản cố định hữu hình" has no dot at all, "2 Vay các tổ chức" the
# same — so the separator is optional.
_ROMAN_RE = re.compile(r"^\s*([ivxlc]+)\s*[.)]?\s+", re.I)
_DIGIT_RE = re.compile(r"^\s*(\d{1,2})\s*[.)]?\s*")
_LETTER_RE = re.compile(r"^\s*([a-z])\s*[.)]\s*", re.I)


def _is_memo(name: str) -> bool:
    """A sub-note hanging off the line above ("- Trong đó lợi thế thương mại"), as opposed to
    a total or a section banner. It is dashed, or opens with "trong đó" / "trong đo"."""
    return name.lstrip().startswith(("-", "–", "—", "+")) or _norm(name).startswith("trong do")


def _split_number(name: str):
    """-> (level, index, token, rest).

    level 1 = roman, 2 = digit, 3 = letter, 0 = neither. `index` is the numeric value (a
    roman becomes an int); `token` is the numbering exactly as printed, lowercased.
    """
    m = _ROMAN_RE.match(name)
    if m and m.group(1).lower() in ROMAN:
        return 1, ROMAN[m.group(1).lower()], m.group(1).lower(), name[m.end():]
    m = _DIGIT_RE.match(name)
    if m:
        return 2, int(m.group(1)), str(int(m.group(1))), name[m.end():]
    m = _LETTER_RE.match(name)
    if m and len(m.group(1)) == 1 and m.group(1).islower():
        return 3, m.group(1).lower(), m.group(1).lower(), name[m.end():]
    return 0, None, "", name


def build_columns(items: List[dict], section: str, start: int,
                  hierarchical: bool, prefix: str = "") -> List[Item]:
    """Turn CafeF's template into canonical column names.

    The numbering lives inside the names, so it is recovered by walking the list in order.
    The three statements are numbered on DIFFERENT principles, and conflating them produces
    nonsense, so each gets its own rule.

    BALANCE SHEET — hierarchical. Roman is the section, digit its child, letter its
    grandchild; a column is `<roman>_<digit>_<letter>_<its own account>`, the parents
    supplying the index and the item supplying the text. Every level is kept exactly as the
    filing prints it — a roman stays roman, a letter stays a letter:

        180  VII. Hoạt động mua nợ   ->  vii_hoat_dong_mua_no
        181   1. Mua nợ              ->  vii_1_mua_no
        189   2. Dự phòng rủi ro …   ->  vii_2_du_phong_rui_ro_hoat_dong_mua_no
        222    a. Nguyên giá TSCĐ    ->  x_1_a_nguyen_gia_tai_san_co_dinh

    Keeping the letter is what separates the three "Hao mòn tài sản cố định" lines (CafeF
    codes 223/226/229): x_1_b, x_2_b, x_3_b.

    INCOME STATEMENT and CASH FLOW — flat. Arabic digits are the component lines and Roman
    numerals are the SUBTOTALS of the lines above them; the two are siblings in one sequence,
    NOT parent and child. Numbering is therefore kept exactly as printed — a digit stays a
    digit, a roman stays roman — which also keeps the two series in separate namespaces so
    "1." and "I." cannot collide:

        1.  Thu nhập lãi …           ->  1_thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu
        I.  Thu nhập lãi thuần       ->  i_thu_nhap_lai_thuan          (= 1 - 2)
        XI. Tổng lợi nhuận trước thuế ->  xi_tong_loi_nhuan_truoc_thue

    CASH FLOW additionally carries its section (`prefix`), because the digits RESTART in each
    one — HDKD runs 1..22 and HDTC starts again at 1 — so a bare "1_" would collide:

        HDKD 1. Thu nhập lãi …       ->  hdkd_1_thu_nhap_lai_va_cac_khoan_thu_nhap_tuong_tu
        HDDT Mua sắm TSCĐ            ->  hddt_mua_sam_tai_san_co_dinh   (HDDT prints no number)
        HDTC 1. Tăng vốn cổ phần     ->  hdtc_1_tang_von_co_phan_tu_phat_hanh_co_phieu

    An unnumbered line is one of two things, and they must not be confused:

      * a TOTAL or a section banner ("TỔNG TÀI SẢN", "B. NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU") —
        it belongs to nothing, so it takes its bare name and RESETS the counters;
      * a MEMO line ("- Trong đó lợi thế thương mại") — it hangs off the item above, so it
        keeps the current parents and must NOT reset them. Treating one as a total orphans
        what follows: the memo under "XII.4 Tài sản Có khác" reset the hierarchy and the next
        line, "5. Các khoản dự phòng…", came out `5_…` instead of `12_5_…`.
    """
    out: List[Item] = []
    roman: Optional[str] = None      # the numbering as printed, e.g. "vii"
    digit: Optional[str] = None
    seen: Dict[str, int] = {}
    head = [prefix] if prefix else []

    for i, it in enumerate(items):
        name = (it.get("name") or "").strip()
        if not name:
            continue
        level, index, token, rest = _split_number(name)
        account = slug(rest)
        if not account:
            continue

        if not hierarchical:
            # flat: the printed numbering, verbatim
            parts = [token] if token else []
        elif level == 1:
            roman, digit = token, None      # the roman stays roman: "vii", not "7"
            parts = [token]
        elif level == 2:
            digit = token
            parts = ([roman] if roman else []) + [token]
        elif level == 3:
            parts = ([roman] if roman else []) + ([digit] if digit else []) + [token]
        elif _is_memo(name):
            parts = ([roman] if roman else []) + ([digit] if digit else [])
        else:
            roman = digit = None
            parts = []

        column = "_".join(head + parts + [account])
        if column in seen:                    # CafeF repeats a name here and there
            seen[column] += 1
            column = f"{column}_{seen[column]}"
        else:
            seen[column] = 1

        out.append(Item(column=column, code=str(it.get("code") or ""), name=name,
                        account=account, section=section, level=level,
                        order=start + i))
    return out


def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": REFERER})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch(report: str, symbol: str = "VCB") -> List[Item]:
    """The canonical chart of accounts for one report, straight from CafeF's own tab.

    The template is the SCHEMA — it is what the tab renders its rows from, and it is the same
    for every ticker on the same accounting template (bank vs non-bank), which is what makes
    a column name mean the same thing across the panel.
    """
    url, sections, shape = TABS[report]
    # Only the balance sheet is a true hierarchy (see build_columns). The cash flow needs its
    # section in the name because its numbering restarts in each one.
    hierarchical = report == BALANCE_SHEET
    with_section = report == CASH_FLOW

    out: List[Item] = []
    for sec in sections:
        q = (f"{url}?symbol={symbol}&pageIndex=1&pageSize=1"
             f"&reportType={sec}&TypeTime=QUY")
        value = _get(q).get("value") or {}
        if shape == "nested":
            items = value["templace"][0]["data"] if value.get("templace") else []
        else:
            items = value.get("templace") or []
        out.extend(build_columns(items, sec, start=len(out),
                                 hierarchical=hierarchical,
                                 prefix=sec.lower() if with_section else ""))
    return out


def columns(report: str, symbol: str = "VCB") -> List[str]:
    return [i.column for i in fetch(report, symbol)]


# ══════════════════════════════════════════════════ which template does a ticker file on?

def raw_items(symbol: str) -> Dict[Tuple[str, str], List[dict]]:
    """(report, section) -> CafeF's template rows, exactly as served."""
    out: Dict[Tuple[str, str], List[dict]] = {}
    for report, (url, sections, shape) in TABS.items():
        for sec in sections:
            q = (f"{url}?symbol={symbol}&pageIndex=1&pageSize=1"
                 f"&reportType={sec}&TypeTime=QUY")
            try:
                value = _get(q).get("value") or {}
            except Exception:
                out[(report, sec)] = []
                continue
            items = (value["templace"][0]["data"]
                     if shape == "nested" and value.get("templace")
                     else value.get("templace") or [])
            out[(report, sec)] = [i for i in items if (i.get("name") or "").strip()]
    return out


def fingerprint(symbol: str) -> Tuple[int, ...]:
    """The line-item count of each section — the ticker's accounting template, in six numbers."""
    items = raw_items(symbol)
    return tuple(len(items.get((r, s), []))
                 for r, (_, secs, _) in TABS.items() for s in secs)


def cash_flow_method(symbol: str) -> Optional[str]:
    """`indirect` | `direct`, from what the operating section opens with.

    A COMPANY chooses this, not a sector and not a template, so it must be read per ticker —
    and per filing, since a company may switch. Everything else in the cash-flow statement is
    the same either way, which is why one schema holds both (see `save`).
    """
    items = raw_items(symbol).get((CASH_FLOW, "HDKD"), [])
    return method_of(items[0].get("name") or "") if items else None


def method_of(first_operating_line: str) -> Optional[str]:
    """The method, from the first line of the operating section — of an API template or of a
    parsed PDF, since both print the same words."""
    n = _norm(first_operating_line)
    if not n:
        return None
    return INDIRECT if INDIRECT_OPENS in n else DIRECT


def detect_template(symbol: str) -> Optional[str]:
    """Which of the four accounting templates this ticker files on.

    FINGERPRINT THE TICKER, DO NOT CLASSIFY IT. GICS says what the business is; the chart of
    accounts says what the filing looks like, and only the second is what a parser needs. The
    two disagree: HVA sits in the securities industry group and files on the CORPORATE
    template. Sector alone is worse still — "Tài chính" spans banks, brokers AND insurers,
    which share nothing.

    Falls back to the shape of the income statement when the counts are unfamiliar, so a
    template CafeF revises does not silently become "unknown".
    """
    fp = fingerprint(symbol)
    hit = FINGERPRINTS.get(fp)
    if hit:
        return hit
    items = raw_items(symbol).get((INCOME_STATEMENT, "KQKD"), [])
    first = _norm(items[0].get("name") or "") if items else ""
    if "thu nhap lai" in first:
        return BANK
    if "doanh thu hoat dong" in first:
        return SECURITIES
    if "doanh thu phi bao hiem" in first:
        return INSURANCE
    if "doanh thu ban hang" in first:
        return CORP
    return None


def save(template: str, out_dir: str, symbol: Optional[str] = None,
         direct_symbol: Optional[str] = None) -> Dict[str, int]:
    """Write `schema_<template>_<report>.csv` for each statement.

    `symbol` may be ANY ticker on the template (defaults to the reference one) — the chart of
    accounts belongs to the template, not the company, which is the point of pinning it down.

    THE CASH-FLOW SCHEMA HOLDS BOTH METHODS. Indirect and direct are near-disjoint — of 19 and
    8 operating lines they share exactly one, the subtotal both converge on — and investing and
    financing are the same lines either way. So one table carries both branches and a filing
    fills the one it used, leaving the other blank (blank, not zero: the company did not report
    those lines). That keeps the statement reconcilable whichever method it used, and is why
    there are 12 schemas and not 16.

    Pass `direct_symbol` — a ticker on the same template that files DIRECT — to fold its
    operating lines in. Without it only the method `symbol` uses is captured.
    """
    import csv
    import os

    if template not in TEMPLATES:
        raise ValueError(f"unknown template {template!r}; expected one of {TEMPLATES}")
    symbol = symbol or REFERENCE[template]

    os.makedirs(out_dir, exist_ok=True)
    counts: Dict[str, int] = {}
    for report in REPORTS:
        items = fetch(report, symbol)

        if report == CASH_FLOW and direct_symbol:
            items = _union_cash_flow(items, fetch(report, direct_symbol))

        path = os.path.join(out_dir, f"{template}_{report}.csv")
        tmp = path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["order", "template", "method", "section", "level", "cafef_code",
                        "column", "as_printed"])
            for it in items:
                w.writerow([it.order, template, getattr(it, "method", ""), it.section,
                            it.level, it.code, it.column, it.name])
        os.replace(tmp, path)
        counts[report] = len(items)
    return counts


def _union_cash_flow(a: List[Item], b: List[Item]) -> List[Item]:
    """Merge two cash-flow charts that differ only in method.

    The operating branches are tagged so they cannot be confused, and the ONE line they share
    — the net-operating subtotal both methods arrive at — stays a single untagged column, so
    the statement reconciles the same way whichever method the filing used. Investing and
    financing are common; they are taken from the first chart (the wording differs slightly
    between tickers, but they are the same lines).
    """
    def ops(items):
        return [i for i in items if i.section == "HDKD"]

    def rest(items):
        return [i for i in items if i.section != "HDKD"]

    ma, mb = _method_of_items(a), _method_of_items(b)
    shared = {i.account for i in ops(a)} & {i.account for i in ops(b)}

    out: List[Item] = []
    for items, method in ((a, ma), (b, mb)):
        for it in ops(items):
            if it.account in shared:
                if any(o.account == it.account and o.section == "HDKD" for o in out):
                    continue                      # the shared subtotal: keep one
                out.append(it)
                continue
            tag = f"hdkd_{method}_" if method else "hdkd_"
            out.append(Item(column=it.column.replace("hdkd_", tag, 1), code=it.code,
                            name=it.name, account=it.account, section=it.section,
                            level=it.level, order=it.order, method=method))
    out += rest(a)
    for n, it in enumerate(out):
        it.order = n
    return out


def _method_of_items(items: List[Item]) -> Optional[str]:
    first = next((i.name for i in items if i.section == "HDKD"), "")
    return method_of(first)
