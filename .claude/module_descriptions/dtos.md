# `src/dtos` — the shared data-transfer objects

> **Read this before touching any dataclass under `src/dtos/`.** Measured 2026-09-06 by reading
> all 7 files and counting every reference across `src/` and `experiment/` (excluding
> `src/kaggle_gpu/.rehearsal/`, which holds copies of the repo, not the repo).
>
> ⚠️ **This package is a MIXED-AGE ATTIC, not one coherent layer.** One of its four sub-packages
> carries the entire database access path (~230 reference sites); one carries the scraper
> concurrency unit (36); and two are **notebook-era leftovers with 0 and 2 live references**.
> Do not read "it is in `dtos/`" as "it is used".

---

## 1. What it is

Plain `@dataclass` containers (plus one hand-written class) that pass structured values between
packages without those packages importing each other. No I/O, no SQL execution, no model code —
the closest thing to it is `Task.run`, which executes a callable, and the `__post_init__`
validators, which raise.

| | |
|---|---|
| files | **7 `.py`**, 469 lines total |
| sub-packages | 4 — `tabular_database_driver_dtos`, `thread_manager_dtos`, `model_dtos`, `config_dtos` |
| `__init__.py` | ⚠️ **none, anywhere** — these are implicit namespace packages. Imports resolve because `src/` is on `sys.path`, so every import is absolute: `from dtos.<sub>.<mod> import X` |
| last commit touching it | `9dc9569d` 2026-06-06 (`task.py`) |
| outward dependencies | `utils.enums`, `utils.constants.RANDOM_SEED`, `logger.logger.Logger`, `numpy`, `torch.nn` |

⚠️ **IT IS NOT A LEAF PACKAGE, AND THE COUPLING IS WORTH KNOWING.**
`BaseTabularDatabaseDto` holds a `Logger` **as a required field**, and `ModelOutputDto` imports
`torch.nn`. So importing anything from `dtos.tabular_database_driver_dtos` pulls in `logger`, and
anything from `dtos.model_dtos` pulls in **torch**. There is no cycle today (`logger/logger.py`
imports only stdlib), but the torch edge means a DTO import can cost seconds in a process that
never trains anything.

---

## 2. Layout, with the measured reference count of each name

Counts are whole-word hits outside `src/dtos/` itself.

| file | lines | last commit | exports | refs |
|---|---|---|---|---|
| `tabular_database_driver_dtos/tabular_database_driver_dtos.py` | 128 | 2026-05-14 | `DataType`, `Condition`, `Column`, `Record`, `DataModel`, `JoinModel`, `ForeignKey` | **100 / 53 / 19 / 9 / 0 / 8 / 2** |
| `tabular_database_driver_dtos/postgre_sql_connection_dto.py` | 26 | 2025-09-14 | `PostgreSQLConnectionDto` | **39** |
| `tabular_database_driver_dtos/base_tabular_database_connection_dto.py` | 27 | 2025-09-14 | `BaseTabularDatabaseDto` | 2 |
| `thread_manager_dtos/task.py` | 95 | 2026-06-06 | `Task` | **36** |
| `model_dtos/model_output_dto.py` | 73 | 2025-11-03 | `ModelOutputDto` | **2** (both in `utils/utils.py`) |
| `model_dtos/model_config_dto.py` | 78 | 2026-03-07 | `ModelConfigDto` | ⚠️ **0** outside `dtos/` |
| `config_dtos/config_dto.py` | 42 | 2026-05-27 | `ConfigDto` | ⚠️ **0** anywhere |

---

## 3. `tabular_database_driver_dtos` — the live sub-package

This is the query-construction vocabulary of `src/tabular_database_driver/` and, through it, of
`orchestration/preprocessor`, `ta`, `sentiment`, `pipeline.freshness` and `utils`. Changing a
field here changes every SQL builder in the repo.

### The connection pair

`BaseTabularDatabaseDto` (`logger`, `host`, `user`, `password`) validates in `__post_init__` —
each empty field is logged **and** raised. `PostgreSQLConnectionDto` extends it with
`port: int = 5432` and `database: str = "postgres"`.

⚠️ **Three things about it are surprising and all three are load-bearing:**

1. **`password` is annotated `list` and is always a `str`.** Every construction site passes
   `os.getenv("POSTGRES_PASSWORD")`. The annotation is wrong and harmless (dataclasses do not
   enforce types), but do not "fix" it by adding a runtime check without reading all the call
   sites.
2. ⚠️ **`port` is declared `int` and comes out of `__post_init__` as a `str`** —
   `self.port = str(port_int)` at [postgre_sql_connection_dto.py:25](../../src/dtos/tabular_database_driver_dtos/postgre_sql_connection_dto.py#L25).
   Anything doing arithmetic or an identity comparison on `.port` after construction gets a
   string. psycopg2 accepts it, which is why nobody has noticed.
3. **The accepted range is `1024 <= port <= 65535`**, so a privileged-port setup raises with a
   message that reads like a type error. Both `int` and `str` inputs are accepted at the door —
   `orchestration/resources.py` passes the raw `os.getenv` string, `pipeline/freshness.py` casts
   to `int`.

⚠️ **The logger is a REQUIRED field before `host`**, so a DTO cannot be built in a scratch script
without constructing a `Logger` first — related to `CLAUDE.md` §5 rule 19, which is the other half
of the same problem (an ephemeral script must `load_dotenv` by absolute path or this DTO raises
`"Password cannot be empty"`).

### The query vocabulary

| name | what it is | note |
|---|---|---|
| `DataType` | ⚠️ **a `@dataclass` with no fields whose members are `@classmethod`s returning SQL type STRINGS** — `DataType.VARCHAR(64)` → `"VARCHAR(64)"` | not an `Enum`, not validated, PostgreSQL-flavoured despite the neutral "tabular database" naming. `DataType.BLOB()` emits `"BLOB"`, which PostgreSQL does not have |
| `Condition` | `column`, `operator: SqlOperator`, `value`, `data_type`, `column_func` | `value` accepts a `list` (for `IN`/`NOT_IN`) and `None` (for `IS NULL`); `column_func` wraps the column, e.g. `"lower"` |
| `JoinModel` | join type + both schemas/tables + **parallel column lists** | the only DTO with real behaviour: `__post_init__` raises on unequal list lengths, and `build_on_clause()` emits the `AND`-joined equality predicate |
| `Column`, `ForeignKey` | DDL description for `create_table` | |
| `Record` / `DataModel` | a row as a list of `(column_name, value, data_type)` | ⚠️ see below |

⚠️ **`DataModel` IS CONSTRUCTED NOWHERE IN THE REPO, AND NEITHER IS `Record`.** They appear only
in the signatures of `postgre_sql_driver.insert` / `.update` and the matching interface methods —
9 mentions, **0 call sites**. The row-by-row insert path they describe lost to `CREATE TABLE AS`;
`CLAUDE.md` §5 rule 15 is the reason (a pandas round-trip turns `numeric` into `Decimal` into
`object` into `VARCHAR`). **Treat `insert(records=…)` as untested code, not as the supported way
to write rows.**

⚠️ **`SqlOperator` and `SqlJoinType` live in `utils/enums.py`, not here** — the DTOs only
reference them. A new operator is a two-file change.

---

## 4. `thread_manager_dtos/task.py` — the scraper concurrency unit

`Task(name, func, *args, callbacks=None, dependencies=None, **kwargs)`, consumed by
[thread_manager.py](../../src/thread_manager/thread_manager.py) and constructed by **six
scrapers** (`cafef_scraper`, `cafef_pdf_scraper`, `cafef_news_scraper`, `cafef_index_scraper`,
`simplize_scraper`, `trading_view_scraper`) plus
[preprocessor.py](../../src/orchestration/preprocessor/preprocessor.py). Every scrape in this
repo goes through it.

`ThreadManager._validate_task` checks a task before accepting it: non-empty unique name, callable
`func`, and — the useful one — `inspect.signature(func).bind(*args, **kwargs)`, so a wrong-arity
task fails at `add_task` rather than inside a worker thread.

⚠️ **Two of `Task`'s features are dormant, and one is inconsistent with its only would-be
producer:**

- **`callbacks` has 0 production call sites.** `run()` requires each entry to be a **3-tuple**
  `(callable, args_tuple, kwargs_dict)` and fans them out on a nested `ThreadPoolExecutor`.
  ⚠️ `ThreadManager.generate_callbacks()` returns **bare callables**, which `run()`'s
  `for cb, extra_args, extra_kwargs in self.callbacks` would raise on. The two shapes disagree
  and neither is exercised — do not assume either half works until you run it.
  ⚠️ The constructor's `if callbacks and not isinstance(callbacks, list)` wrap means a **single
  bare function** is silently accepted at construction and only explodes at `run()`.
- **`dependencies` has 0 production call sites**, though `ThreadManager.execute` does implement
  the scheduling for it (a task runs once every name in `dependencies` is in `successful_tasks`).

⚠️ **`run()` `print`s rather than logging** — `f"Executing task: {self.name}"`, and
`f"Callback failed: {e}"` for a callback exception it otherwise **swallows**. That is the one
place a scrape failure can vanish into stdout instead of `logs/app.log`. It also predates the
`xx.x% - task - sub-task - detail` progress convention (`CLAUDE.md` §8) and does not follow it.

⚠️ **The file carries an `if __name__ == "__main__":` demo block** — 45 of its 95 lines. It is a
scratch test, not a test suite; there is no `tests/` for this package.

---

## 5. `model_dtos` and `config_dtos` — the notebook-era half

⚠️ **NEITHER IS PART OF THE CURRENT MODEL CHAIN.** Stage 7 of `CLAUDE.md` §3b is
`python -m model.lstm --config <cfg>`: a run is configured by a **YAML file** and described by the
run folder's config + `metadata.json`, and `d`/`h` come from the source TABLE NAME. None of that
passes through these dataclasses.

| | |
|---|---|
| `ModelConfigDto` | 20 fields (entity/project/architecture, windowing, training, scaler, metric, device, seed), `to_dict()` unwrapping every `Enum` to `.value`, `format_config()` printing indented JSON. ⚠️ **0 references outside `dtos/`** — it survives only because `ModelOutputDto` holds one |
| `ModelOutputDto` | model + `state_dict` + config + loss histories + `y_pred` / `y_pred_denorm` / `y_true` + `mape`. ⚠️ **1 consumer: `utils.plot_model_result()` ([utils.py:568](../../src/utils/utils.py#L568))**, itself only reachable from notebooks |
| `ConfigDto` | 21 fields — data window dates, `lookback_window_size` / `forecast_window_size` / `stride`, training hyper-parameters. ⚠️ **0 references anywhere in the repo** — fully dead |

⚠️ **`ModelAchitectureType` is misspelled** (no `r` in "Architecture") in `utils/enums.py` and is
referenced by both `ConfigDto` and `ModelConfigDto`. It is a public identifier, so renaming it is
a three-file change — and the enum only knows `lstm` and `cnn`: the seven-arm sweep of
`CLAUDE.md` §6-0-ter-2 (`gbt`, `bilstm`, `cnnlstm`, `tcn`, `transformer`) never went through it.

⚠️ **`ModelOutputDto.to_dict()` serialises the FULL prediction vectors** (`y_pred`,
`y_pred_denorm`, `y_true`, each `.tolist()`). On a panel run that is hundreds of thousands of
floats in one JSON blob. The live chain writes `predictions_*.csv` instead, which is what
`result_evaluator --rescore` reads.

---

## 6. Who imports what (reverse map, measured)

| importer | takes |
|---|---|
| `tabular_database_driver/postgre_sql_driver.py`, `…_interface.py` | the whole query vocabulary + both connection DTOs |
| `orchestration/preprocessor/preprocessor.py` | query vocabulary (`import *`), `PostgreSQLConnectionDto`, `Task` |
| `orchestration/resources.py` | `PostgreSQLConnectionDto` |
| `pipeline/freshness.py` | `PostgreSQLConnectionDto` |
| `ta/ta_functions.py` | `PostgreSQLConnectionDto` + query vocabulary |
| `sentiment/run_{,jump_,reaction_,weekly_}prototype.py` | `PostgreSQLConnectionDto` + query vocabulary |
| `utils/utils.py` | query vocabulary + `ModelOutputDto` |
| 6 × `web_scraper/*_scraper.py` | `Task` |
| `model/cross_sectional/xsec_vn30.ipynb` | `PostgreSQLConnectionDto` |

⚠️ **Four importers use `from … import *`** — `preprocessor.py`, `postgre_sql_driver.py`,
`tabular_database_driver_interface.py`, and `thread_manager.py` for `Task`. **Adding a name to
`tabular_database_driver_dtos.py` injects it into those namespaces**, so a new DTO called
`Column`, `Record` or anything already defined downstream shadows silently.

---

## 7. If you change something here

- **A field on `Condition`, `Column`, `JoinModel` or `DataType`** touches every SQL builder in the
  repo through those `import *` sites. There are no tests over this package; the cheapest check is
  `python -m pipeline.freshness --layer silver` (1 s, real queries) plus one `filter/universe`
  materialisation.
- **`PostgreSQLConnectionDto`** is constructed from `.env` in ~39 places under two different
  conventions for `port` (raw string vs `int(...)`). Any new validation must accept both.
- **`Task`** is the scrape path. Its dormant `callbacks` / `dependencies` halves are the two
  places where a change is unverifiable from the current call sites — exercise them explicitly or
  leave them alone.
- **`ConfigDto` / `ModelConfigDto`** are removable as far as static references go, but that is a
  decision to record, not a cleanup: they are the only surviving description of the pre-Dagster
  notebook run contract. ⚠️ `CLAUDE.md` §5b and `RPR-1` record what the last deletion cost —
  check `git log` first, and write the removal down where it will be read.
