"""Which Kaggle ACCOUNT a job runs under — discovered, measured, and chosen.

⚠️ **THE NAMING CONVENTION, AND IT IS THE FIRST HALF OF THE FIX.** One token per account,
in a variable named after that account in UPPER CASE:

    KAGGLE_API_TOKEN_<USERNAME>=KGAT_xxxx

`discover()` takes the suffix, lower-cased, as the account's LABEL — so `.env` reads

    KAGGLE_API_TOKEN_LYDUCTRUNG=...        -> label `lyductrung`
    KAGGLE_API_TOKEN_DUCTRUNG180200=...    -> label `ductrung180200`

and **there is no bare `KAGGLE_API_TOKEN` on this machine at all.** ⚠️ Until 2026-09-08 there
was, and the choice between the two accounts was made by *which variable happened to be
called `KAGGLE_API_TOKEN`* — a name in a dotenv file deciding where hours are spent and who
owns the kernel. Naming each after its account makes the wrong one impossible to pick by
accident and impossible to pick silently: `kagglesdk` reads only the bare name, so one token
has to be COPIED into it, and that copy is now this module's decision rather than an
alphabetical accident. `Status.misnamed` reports a variable whose account does not match its
name, because a name that lies is worse than no name.

**Two things made the old arrangement untenable, both measured 2026-09-08:**

| measured | `ductrung180200` | `lyductrung` |
|---|---|---|
| GPU quota left | **3.59 h** of 30.00 h | **30.00 h** of 30.00 h |
| resets | 2026-09-12T00:00:00 | 2026-09-12T00:00:00 |

1. **The account every code path used had 3.59 h left**, which is under a whole-ticker OCR
   run (HOSE_FPT was 185 min over 71 filings on a T4). A run started on it dies partway with
   the filings parsed and nothing pulled.
2. ⚠️ **AND IT DOES NOT OWN THE CONFIGURED JOBS.** Every `id` in `kaggle_config.json` reads
   `lyductrung/...`, i.e. the OTHER account — so `python -m kgpu run cross-sectional` would
   have pushed to a kernel slug its credentials cannot write, and Kaggle answers that with a
   403 **after** the payload has been uploaded.

⚠️ **THE CHOICE IS NOT ALWAYS A QUOTA QUESTION, AND THAT IS THE WHOLE DESIGN.**
`select_for_job` (a job whose id names an owner) and `select_for_name` (a job whose owner is
derived) resolve it in three tiers, most binding first:

    1. THE JOB NAMES AN OWNER  -> that account, and quota gets NO vote. A file-borne job's
                                  `id` is `lyductrung/mt-...`; only `lyductrung` can push it.
                                  Short on hours is reported as a WARNING, never as a switch:
                                  switching would push to a slug the credentials do not own.
    2. THE LEDGER NAMES ONE    -> a job whose owner is DERIVED (every `pdf_ocr` job) and which
                                  this machine has already pushed keeps the account that owns
                                  the live kernel, as long as it still covers the estimate.
                                  ⚠️ Switching mid-ticker orphans the running kernel on the
                                  other account and re-uploads the whole filings payload.
    3. OTHERWISE, BY QUOTA     -> `by_quota` below, which is where the criterion lives.

Read-only throughout: `quota_view` spends nothing and took 2-3 s per account when measured.
`python -m kgpu accounts` prints the survey.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from .config import PKG_ROOT, REPO_ROOT, JobConfig

#: Every environment variable holding a Kaggle access token starts with this. The bare name is
#: the one the Kaggle SDK itself reads (`kagglesdk.kaggle_env.get_access_token_from_env`), so a
#: second account has to be carried under a SUFFIXED name and copied into the bare one before
#: the client authenticates — which is what `activate` does.
TOKEN_PREFIX = "KAGGLE_API_TOKEN"

#: The label of the account carried in the bare `KAGGLE_API_TOKEN`. ⚠️ Kept for a machine
#: that still uses the unsuffixed variable; this one does not, and a label under the
#: 2026-09-08 convention IS the username.
DEFAULT_LABEL = "default"

#: Which account last pushed which job, from THIS machine. Gitignored with the rest of
#: `.env`-shaped state; a missing file means "no history", never an error.
LEDGER = PKG_ROOT / ".accounts.json"

#: ⚠️ **NOMINAL, AND MEASURED ON ONE TICKER.** HOSE_FPT was a 185-minute round trip over 71
#: filings on a T4 (CLAUDE.md §8) — 2.6 min per filing, averaged over a ticker whose documents
#: ranged from one OCR pass to the full cascade. It is an AVERAGE and not a bound: a single
#: filing that defeats every layer has cost 33 min on its own. What it is for is sizing a
#: quota requirement, where being wrong low is the expensive direction — hence `SAFETY`.
HOURS_PER_DOCUMENT = 2.6 / 60

#: Payload export, upload, queue and the worker's own pip install of the pinned OCR stack —
#: paid once per round trip whatever the document count. ⚠️ Nominal: nothing has measured the
#: queue, which is Kaggle's and varies.
FIXED_OVERHEAD_HOURS = 0.5

#: How much more than the estimate an account must hold before it is considered a fit.
#: ⚠️ A Kaggle session is KILLED when the weekly quota runs out under it, and an OCR run that
#: dies mid-flight leaves the filings parsed inside a kernel nothing will pull — so the margin
#: buys back the one failure this module exists to prevent.
SAFETY = 1.25


# ----------------------------------------------------------------- discovery


@dataclass(frozen=True)
class Account:
    """One set of credentials, named by the variable it was found in."""

    var: str        # e.g. "KAGGLE_API_TOKEN_LYDUCTRUNG"
    label: str      # the suffix, lower-cased — e.g. "lyductrung"; "default" for the bare var
    token: str

    def __str__(self) -> str:                       # what a log line shows
        # ⚠️ The VARIABLE, not the label — since 2026-09-08 a token is named after its
        # account, so the label repeats the username and `label (var)` printed one name
        # three times on a line. The variable is the thing you edit in `.env`.
        return self.var


@dataclass
class Status:
    """What one account answered when asked. `error` set means it answered nothing."""

    account: Account
    user: Optional[str] = None
    gpu_left: Optional[float] = None
    gpu_total: Optional[float] = None
    tpu_left: Optional[float] = None
    resets: Optional[datetime] = None
    error: Optional[str] = None

    @property
    def usable(self) -> bool:
        """⚠️ A GPU quota that came back `None` is UNKNOWN, not zero (§5 rule 2). Either way
        it cannot be ranked against a number, so it is not a candidate for the quota tier."""
        return self.error is None and self.user is not None and self.gpu_left is not None

    @property
    def misnamed(self) -> bool:
        """⚠️ The variable is named after an account it does not authenticate as.

        Only meaningful under the 2026-09-08 convention (`KAGGLE_API_TOKEN_<USERNAME>`), so
        the bare variable — label `default` — is exempt. It is a WARNING and never a
        refusal: a token that works is a token that works, and the name is documentation.
        Worth saying out loud, because a name that lies is how the wrong account gets picked
        by a human reading `.env` rather than by this module.
        """
        return (self.user is not None and self.account.label != DEFAULT_LABEL
                and self.account.label != self.user.lower())

    def line(self) -> str:
        if self.error:
            return f"  {self.user or '?':<18} {str(self.account):<34} FAILED: {self.error}"
        left = f"{self.gpu_left:.2f}h" if self.gpu_left is not None else "unknown"
        total = f"{self.gpu_total:.2f}h" if self.gpu_total is not None else "?"
        resets = self.resets.isoformat() if self.resets else "?"
        return (f"  {self.user or '?':<18} {str(self.account):<34} "
                f"GPU {left:>8} of {total:<8} resets {resets}"
                + ("   ⚠️ the variable names a different account" if self.misnamed else ""))


def _dotenv(path: Path) -> Dict[str, str]:
    """Read a dotenv file WITHOUT touching `os.environ`.

    ⚠️ `load_dotenv` is the wrong tool here: it exports, and this function is called to
    *survey* accounts before one has been chosen. Exporting during a survey is how the
    last file read would silently become the active account.
    """
    try:
        from dotenv import dotenv_values
    except ImportError:                             # pragma: no cover — dotenv is a hard dep
        return {}
    if not path.exists():
        return {}
    return {k: v for k, v in dotenv_values(path).items() if v}


def discover() -> List[Account]:
    """Every account this machine can authenticate as, package `.env` winning on a conflict.

    ⚠️ **THE PRECEDENCE IS `load_credentials`', DELIBERATELY.** That function loads the
    package `.env` first with `override=False`, so the package file is authoritative; a
    survey that ranked them the other way would report an account the run would not use.

    ⚠️ **`os.environ` IS READ LAST AND WINS NOTHING.** A token exported in the shell is a
    real account and is listed — but a variable this module set itself (`activate` writes
    the bare `KAGGLE_API_TOKEN`) must not be able to shadow a file, or one selection would
    change what the next survey sees.
    """
    found: Dict[str, Account] = {}
    sources = [_dotenv(PKG_ROOT / ".env"), _dotenv(REPO_ROOT / ".env"), dict(os.environ)]
    seen_tokens: Dict[str, str] = {}                # token -> var that first carried it
    for source in sources:
        for var, token in source.items():
            if not var.startswith(TOKEN_PREFIX) or not token:
                continue
            suffix = var[len(TOKEN_PREFIX):].lstrip("_")
            label = suffix.lower() or DEFAULT_LABEL
            if label in found:                      # an earlier source already named it
                continue
            # ⚠️ The SAME token under two names is one account, not two. It happens the
            # moment `activate` copies a suffixed token into the bare variable and something
            # surveys afterwards — listing it twice would double-count the hours on offer.
            if token in seen_tokens:
                continue
            seen_tokens[token] = var
            found[label] = Account(var=var, label=label, token=token)
    return sorted(found.values(), key=lambda a: (a.label != DEFAULT_LABEL, a.label))


def activate(account: Account) -> None:
    """Make `account` the one the Kaggle client will authenticate as.

    ⚠️ **THE LEGACY PAIR IS CLEARED, NOT LEFT.** `KaggleApi.authenticate` tries the access
    token FIRST and falls back to `KAGGLE_USERNAME`+`KAGGLE_KEY`; leaving a stale pair behind
    a token that later fails to introspect would silently authenticate as a third identity.

    ⚠️ **AND IT MUST HAPPEN BEFORE `KaggleApi()` IS CONSTRUCTED.** `authenticate` reads the
    environment once and caches the result on the instance, so an api object built earlier in
    the session keeps the old account. `runner._api()` builds a fresh one per call, which is
    what makes switching mid-session safe there.
    """
    for var in ("KAGGLE_USERNAME", "KAGGLE_KEY"):
        os.environ.pop(var, None)
    os.environ[TOKEN_PREFIX] = account.token
    os.environ["KGPU_ACCOUNT"] = account.label


def active_label() -> Optional[str]:
    """The label `activate` last set in this process, or None if nothing was selected."""
    return os.environ.get("KGPU_ACCOUNT")


# ------------------------------------------------------------------- probing


def probe(account: Account) -> Status:
    """Authenticate as `account` and read its weekly accelerator quota. Read-only.

    ⚠️ **IT RESTORES THE ENVIRONMENT IT FOUND.** A survey is not a selection: probing three
    accounts must leave the process authenticating exactly as it did before, or the ORDER of
    the survey would decide the run.

    ⚠️ **A FAILURE IS RECORDED, NEVER RAISED.** An expired token is a fact about one account
    and the other one may be fine; raising here would take a survey down over a credential
    nothing was going to use anyway.
    """
    saved = {var: os.environ.get(var)
             for var in (TOKEN_PREFIX, "KAGGLE_USERNAME", "KAGGLE_KEY", "KGPU_ACCOUNT")}
    try:
        activate(account)
        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()
        user = (getattr(api, "config_values", {}) or {}).get("username")
        response = api.quota_view()
        status = Status(account=account, user=user,
                        resets=getattr(response, "quota_refresh_time", None))
        for attr, left, total in (("gpu_quota", "gpu_left", "gpu_total"),
                                  ("tpu_quota", "tpu_left", None)):
            quota = getattr(response, attr, None)
            if quota is None:
                continue
            used_h = quota.time_used.total_seconds() / 3600
            total_h = quota.total_time_allowed.total_seconds() / 3600
            setattr(status, left, max(0.0, total_h - used_h))
            if total:
                setattr(status, total, total_h)
        return status
    except Exception as exc:                        # noqa: BLE001 — any failure is "no answer"
        return Status(account=account, error=f"{type(exc).__name__}: {exc}")
    finally:
        for var, value in saved.items():
            if value is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = value


def survey(accounts: Optional[Sequence[Account]] = None) -> List[Status]:
    """Probe every discovered account. ~2-3 s each, measured 2026-09-08. Spends no quota."""
    return [probe(a) for a in (accounts if accounts is not None else discover())]


def report(statuses: Sequence[Status]) -> List[str]:
    """The survey as printable lines — one per account, plus what is missing."""
    if not statuses:
        return [f"  no {TOKEN_PREFIX}* found in {PKG_ROOT / '.env'} or {REPO_ROOT / '.env'}"]
    return [s.line() for s in statuses]


# ----------------------------------------------------------------- the ledger


def _ledger() -> Dict[str, Dict[str, str]]:
    try:
        return json.loads(LEDGER.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return {}


def owner_of(job_name: str) -> Optional[str]:
    """The Kaggle username that last pushed `job_name` from this machine, if any."""
    return (_ledger().get(job_name) or {}).get("user")


def record(job_name: str, user: str) -> None:
    """Remember who pushed `job_name`, so a later session does not orphan its kernel.

    ⚠️ **THIS IS THE ONLY THING THAT SURVIVES A SESSION**, and it is why it is written at
    PUSH and not at selection: a job that was only planned owns no kernel, and recording an
    intention would pin every later run to an account that never ran anything.
    """
    data = _ledger()
    data[job_name] = {"user": user, "at": datetime.now().isoformat(timespec="seconds")}
    LEDGER.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


# --------------------------------------------------------------- the criterion


def estimate_hours(n_documents: int) -> float:
    """A NOMINAL GPU-hour cost for a PDF-OCR round trip over `n_documents` filings.

    ⚠️ **NOMINAL — read `HOURS_PER_DOCUMENT` before leaning on it.** It is one ticker's
    average (HOSE_FPT, 71 filings, 185 min) applied to a count, and the spread behind that
    average is a filing accepted at layer 1 in ~1 min against one that defeated the cascade
    in 33. It sizes a quota requirement; it does not predict a runtime.
    """
    return FIXED_OVERHEAD_HOURS + max(0, int(n_documents)) * HOURS_PER_DOCUMENT


def by_quota(statuses: Sequence[Status], need_hours: float = 0.0) -> Status:
    """Pick an account on remaining GPU hours alone. The criterion, in one place.

    ⚠️ **TIGHTEST FIT, NOT MOST REMAINING — and the reason is the shape of the workload.**
    The quota is per account, per week, and it does not pool: two accounts holding 15 h each
    cannot run the 20-hour job that one account holding 30 h can. Spending the SMALLEST
    balance that still covers the estimate therefore drains one account first and keeps a
    whole untouched week on the other, which is the only state in which the longest run this
    repo has (a ~70-filing ticker) is still startable. "Most remaining" does the opposite: it
    alternates, and ends the week with two half-quotas and nothing that fits.

    The requirement is `need_hours * SAFETY + FIXED_OVERHEAD` because a session KILLED by an
    exhausted quota loses everything the kernel had parsed.

    ⚠️ **WHEN NOTHING FITS IT RETURNS THE LARGEST BALANCE AND SAYS SO** rather than raising.
    An estimate is nominal (`estimate_hours`) and a ticker whose filings all accept at layer 1
    can finish well inside a balance the estimate called short — refusing on a nominal number
    would be this repo's own §5 rule 2 in reverse. The caller prints the warning; the operator
    decides.

    ⚠️ **AND WITH NO ESTIMATE IT IS MOST-REMAINING, NOT TIGHTEST FIT.** Tightest fit is an
    answer to *"which balance covers this?"* and there is no question without a demand;
    packing against a requirement of zero would hand every unsized run the emptiest account,
    which is the exact failure this module was written for.
    """
    usable = [s for s in statuses if s.usable]
    if not usable:
        failed = "; ".join(f"{s.account}: {s.error or 'no quota reported'}" for s in statuses)
        raise RuntimeError(
            "no Kaggle account answered with a usable GPU quota — "
            + (failed or f"nothing named {TOKEN_PREFIX}* was found"))
    if need_hours <= 0:
        return max(usable, key=lambda s: (s.gpu_left, s.account.label))
    required = need_hours * SAFETY + FIXED_OVERHEAD_HOURS
    fits = [s for s in usable if s.gpu_left >= required]
    if fits:
        # ⚠️ The tie-break is the SOONEST reset, then the label: hours that expire first are
        # hours to spend first, and a stable last key keeps two identical accounts from
        # choosing differently on two runs of the same notebook.
        return min(fits, key=lambda s: (s.gpu_left,
                                        s.resets.timestamp() if s.resets else 0.0,
                                        s.account.label))
    return max(usable, key=lambda s: (s.gpu_left, s.account.label))


def owner_in(job_id: str) -> Optional[str]:
    """The `owner` half of a Kaggle `owner/slug`, or None if the id carries no owner."""
    return job_id.split("/")[0] if "/" in job_id else None


@dataclass
class Choice:
    """A resolved account, with the reason — which is what gets printed and recorded."""

    status: Status
    tier: str                   # "owner" | "ledger" | "quota"
    reason: str
    warnings: List[str]

    @property
    def user(self) -> Optional[str]:
        return self.status.user

    def lines(self) -> List[str]:
        out = [f"account     : {self.status.user}   ({self.status.account})",
               f"              {self.reason}"]
        if self.status.misnamed:
            out.append("              ⚠️ that variable is named after a DIFFERENT account")
        if self.status.gpu_left is not None:
            resets = self.status.resets.isoformat() if self.status.resets else "?"
            # ⚠️ `gpu_total` can be absent where `gpu_left` is not — the response carries them
            # separately — and a bare `:.2f` on None raises inside the one call whose job is to
            # explain the choice. A missing denominator is printed as missing.
            total = (f"{self.status.gpu_total:.2f}h" if self.status.gpu_total is not None
                     else "an unreported total")
            out.append(f"quota       : {self.status.gpu_left:.2f}h GPU left of "
                       f"{total}, resets {resets}")
        out += [f"⚠️ {w}" for w in self.warnings]
        return out


def select(*, need_hours: float = 0.0, require_user: Optional[str] = None,
           prefer_user: Optional[str] = None, force_label: Optional[str] = None,
           statuses: Optional[Sequence[Status]] = None) -> Choice:
    """Choose an account, ACTIVATE it, and return the choice with its reason.

    `require_user` is tier 1 (the job names an owner), `prefer_user` tier 2 (the ledger),
    `need_hours` feeds tier 3. `force_label` is the operator overriding all three.
    """
    accounts = discover()
    if not accounts:
        raise RuntimeError(
            f"no Kaggle token found. Put `{TOKEN_PREFIX}_<YOUR_USERNAME>=...` in "
            f"{PKG_ROOT / '.env'} — one variable per account, named after it in UPPER CASE.")
    statuses = list(statuses) if statuses is not None else survey(accounts)
    by_label = {s.account.label: s for s in statuses}
    by_user = {s.user: s for s in statuses if s.user}
    warnings: List[str] = [f"{s.account} did not authenticate: {s.error}"
                           for s in statuses if s.error]

    def _finish(status: Status, tier: str, reason: str) -> Choice:
        activate(status.account)
        extra = list(warnings)
        if status.gpu_left is not None and need_hours:
            required = need_hours * SAFETY + FIXED_OVERHEAD_HOURS
            if status.gpu_left < required:
                extra.append(
                    f"{status.user} holds {status.gpu_left:.2f}h GPU against an estimated "
                    f"{need_hours:.2f}h (+{SAFETY:.2f}x safety +{FIXED_OVERHEAD_HOURS:.2f}h "
                    f"overhead = {required:.2f}h). The estimate is NOMINAL "
                    f"(`estimate_hours`), but a session killed by an exhausted quota loses "
                    f"everything the kernel had parsed.")
        return Choice(status=status, tier=tier, reason=reason, warnings=extra)

    if force_label:
        status = by_label.get(force_label.lower())
        if status is None:
            raise ValueError(
                f"no account labelled {force_label!r}; discovered "
                f"{sorted(by_label)} from {TOKEN_PREFIX}*")
        if status.error:
            raise RuntimeError(f"account {force_label!r} did not authenticate: {status.error}")
        return _finish(status, "forced", f"forced by label {force_label!r}")

    if require_user:
        status = by_user.get(require_user)
        if status is None:
            raise RuntimeError(
                f"this job's kernel is owned by {require_user!r} and no token on this "
                f"machine authenticates as them (found {sorted(by_user)}). Kaggle answers a "
                f"push to a slug you do not own with a 403 — after the payload is uploaded.")
        return _finish(status, "owner",
                       f"the job id names {require_user} — quota gets no vote here")

    required = need_hours * SAFETY + FIXED_OVERHEAD_HOURS if need_hours > 0 else 0.0

    if prefer_user and prefer_user in by_user:
        status = by_user[prefer_user]
        if status.usable and status.gpu_left >= required:
            return _finish(status, "ledger",
                           f"{prefer_user} already pushed this job from this machine — "
                           f"switching would orphan that kernel and re-upload the payload")
        held = f"{status.gpu_left:.2f}h" if status.gpu_left is not None else "an unknown balance"
        warnings.append(
            f"{prefer_user} pushed this job before but holds {held} against {required:.2f}h "
            f"needed — choosing on quota instead. A kernel and a payload dataset already "
            f"exist under {prefer_user}; this run makes a SECOND set, and the first one's "
            f"results stay where they are.")

    status = by_quota(statuses, need_hours)
    fits = status.gpu_left is not None and status.gpu_left >= required
    if need_hours <= 0:
        reason = (f"most GPU hours left of {len(statuses)} — no size was estimated, so the "
                  f"criterion has nothing to fit against")
    elif fits:
        reason = (f"the smallest balance that still covers {required:.2f}h (tightest fit — "
                  f"it keeps one whole week free on the other account)")
    else:
        reason = (f"most GPU hours left of {len(statuses)} — ⚠️ NONE covers the estimated "
                  f"{required:.2f}h")
    return _finish(status, "quota", reason)


def select_for_job(cfg: JobConfig, *, need_hours: float = 0.0,
                   force_label: Optional[str] = None,
                   statuses: Optional[Sequence[Status]] = None) -> Choice:
    """Choose the account for a job whose `id` ALREADY NAMES ITS OWNER — i.e. a file-borne
    job out of `kaggle_config.json`, where every id reads `lyductrung/...`.

    ⚠️ **DO NOT CALL THIS WITH A `pdf_ocr` CONFIG.** That id is DERIVED from whoever is
    authenticated at build time (`pdf_ocr.job` calls `kaggle_user()`), so passing one here
    would read back the account that was already active and call it a requirement — the
    criterion would then only ever confirm the status quo. A derived job selects FIRST, with
    `select_for_name`, and is built SECOND.
    """
    owner = owner_in(cfg.id)
    if owner is None:                               # a slug with no owner cannot be pushed
        raise ValueError(f"job {cfg.name!r} has an id with no owner: {cfg.id!r}")
    return select(need_hours=need_hours, require_user=owner,
                  force_label=force_label, statuses=statuses)


def select_for_name(job_name: str, *, need_hours: float = 0.0,
                    force_label: Optional[str] = None,
                    statuses: Optional[Sequence[Status]] = None) -> Choice:
    """Choose the account for a job whose owner is DERIVED — every `pdf_ocr` job.

    ⚠️ **CALL THIS BEFORE `pdf_ocr.job(...)`, NEVER AFTER.** The kernel slug, the payload
    dataset slug and the pull all follow whoever is authenticated when the config is built;
    selecting afterwards changes the credentials and leaves the config naming the other
    account, which Kaggle answers with a 403 once the payload is already up.

    `job_name` is `pdf_ocr.job_name(...)` — the same string `cfg.name` will hold, and the
    ledger's key, so a ticker resumed next week keeps the account that owns its kernel.
    """
    return select(need_hours=need_hours, prefer_user=owner_of(job_name),
                  force_label=force_label, statuses=statuses)
