# Deploy-time preprocessing PoC — scoping doc

**Status:** investigation in progress; benchmark numbers pending. Engineering
scoping doc, not user-facing — kept outside the Sphinx toctree. The
user-facing matrix is at `docs/optimize_performance/deploy_time_preprocessing.rst`.

## Why this PoC exists

Cosmos may (re)compute three dbt artefacts on every DAG parse or task start:

| Artefact | What it is | Where it's computed today |
|---|---|---|
| **dbt graph** | Cosmos `DbtGraph` object (nodes + filtered_nodes), `cosmos/dbt/graph.py:418` | `DbtGraph.load()` at parse, from `cosmos/converter.py:320-331`. Rebuilt on every parse regardless of `LoadMode`. |
| **dbt_packages/** | dbt package install dir | `DbtGraph.run_dbt_deps()` (`cosmos/dbt/graph.py:824`) at parse when `install_dbt_deps=True`; per-task when operator `install_deps=True` |
| **partial_parse.msgpack** | dbt's own parse cache | dbt-internal; staged into temp run dirs at `cosmos/dbt/graph.py:912-920`; cached at `cosmos/cache.py:173-214` |

Crucially, the Cosmos `DbtGraph` is *not* `manifest.json`. `LoadMode.DBT_MANIFEST`
precomputes the dbt-side input but Cosmos still walks it on every parse to
build the `DbtGraph`. The PoC asks whether the residual construction cost
is worth eliminating via a serialised `DbtGraph`. First deliverable:
measure headline cases and decide which artefacts are worth automating.

## Methodology

Time recorded for a single LOCAL DAG (`example_dbt_dag`) against
`fhir-dbt-analytics` (185 models) in `cosmos-benchmark`. Parse-time comes
from `cosmos/converter.py:341`'s `It took %.3gs to parse the dbt project
for DAG using ...` log in the DAG-processor pod; DAG runtime + worker
CPU/memory from `run-complex-test.sh` → `post-process/report-dag-run-pool-metrics.sh`.

The log measures total `DbtGraph.load()` wall time. Four measurement pairs
isolate dbt-side cost from Cosmos-side cost:

1. **dbt graph, dbt-side win.** Baseline `LoadMode.DBT_LS`; variant adds
   `RUN dbt parse` + `LoadMode.DBT_MANIFEST`. Delta = dbt-invocation cost.
2. **dbt graph, Cosmos-side floor.** Under `LoadMode.DBT_MANIFEST` the log
   time is the residual cost of building `DbtGraph` from a prebuilt
   manifest — the floor a serialised-`DbtGraph` mechanism would aim
   toward zero. Tells us whether to build one.
3. **dbt_packages.** Baseline pre-installs via `RUN dbt deps`; variant
   removes that line and sets `install_dbt_deps=True`.
4. **partial_parse.** Baseline keeps `target/partial_parse.msgpack`;
   variant appends `RUN rm -f target/partial_parse.msgpack`. If signal is
   zero under `DBT_MANIFEST` (expected — dbt isn't invoked at parse),
   re-run under `DBT_LS` to isolate the cold-deploy effect.

## Open benchmark work

Changes needed in `astronomer/cosmos-benchmark`. Not applied — require
BigQuery credentials + a local kind cluster (`~10 vCPU / 20 GiB`) the
author doesn't have set up.

### Change 1 — `benchmark/Dockerfile`: build-arg toggles

```dockerfile
ARG PRECOMPUTE_DEPS=1
ARG PRECOMPUTE_MANIFEST=0
ARG PRECOMPUTE_PARTIAL_PARSE=0

RUN if [ "$PRECOMPUTE_DEPS" = "1" ]; then dbt deps --no-use-colors; fi
RUN if [ "$PRECOMPUTE_MANIFEST" = "1" ]; then dbt parse --no-use-colors; fi
RUN if [ "$PRECOMPUTE_MANIFEST" = "1" ] && [ "$PRECOMPUTE_PARTIAL_PARSE" = "0" ]; then \
        rm -f target/partial_parse.msgpack; \
    fi
```

Build six images:

```bash
docker build --build-arg PRECOMPUTE_DEPS=0 -t cosmos-bench:no-deps .
docker build --build-arg PRECOMPUTE_DEPS=1 -t cosmos-bench:with-deps .          # current baseline
docker build --build-arg PRECOMPUTE_MANIFEST=1 -t cosmos-bench:with-manifest .
docker build --build-arg PRECOMPUTE_MANIFEST=0 -t cosmos-bench:no-manifest .    # same as with-deps
docker build --build-arg PRECOMPUTE_MANIFEST=1 --build-arg PRECOMPUTE_PARTIAL_PARSE=1 -t cosmos-bench:with-pp .
docker build --build-arg PRECOMPUTE_MANIFEST=1 --build-arg PRECOMPUTE_PARTIAL_PARSE=0 -t cosmos-bench:no-pp .
```

Covers the dbt-side component only — measurement 2 needs no Dockerfile
change since no serialised-`DbtGraph` mechanism exists yet.

### Change 2 — `benchmark/dags/cosmos_dags.py`: manifest variant

Make the existing DAG env-toggleable:

```python
import os
from pathlib import Path
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = Path(__file__).parent.parent
USE_MANIFEST = os.getenv("COSMOS_BENCH_USE_MANIFEST", "0") == "1"
manifest_path = (
    (DBT_PROJECT_PATH / "target" / "manifest.json") if USE_MANIFEST else None
)
load_method = LoadMode.DBT_MANIFEST if USE_MANIFEST else LoadMode.DBT_LS

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    install_dbt_deps=os.getenv("COSMOS_BENCH_INSTALL_DEPS", "0") == "1",
    manifest_path=manifest_path,
)
render_config = RenderConfig(test_behavior=TestBehavior.NONE, load_method=load_method)
```

### Change 3 — `benchmark/post-process/report-parse-time.sh`

Extractor mirroring `report-dag-run-pool-metrics.sh`:

```bash
#!/usr/bin/env bash
# Usage: report-parse-time.sh <dag_id> <start_iso> <end_iso>
set -euo pipefail
DAG_ID="$1"; START="$2"; END="$3"

kubectl --context kind-kind -n airflow logs \
    -l component=dag-processor --tail=5000 --since-time="$START" \
  | grep -F "Cosmos performance" \
  | grep -F "($DAG_ID)" \
  | sed -E 's/.* It took ([0-9.]+)s to parse the dbt project for DAG using (.+)$/\1,\2/' \
  | tail -1
```

`run-complex-test.sh` appends to `$METRICS_CSV` with new columns
`parse_time_s, load_mode`.

### Change 4 — `benchmark/results/preprocessing-poc.csv`

After the six runs, commit the CSV in-repo so it stays linked from this doc.

## Automation mechanisms (preliminary)

Mechanisms for shipping artefacts; per-artefact gating is in "Sub-tickets".

- **Otto / Astro CLI deploy hook.** Build-time hook runs `dbt deps && dbt parse`
  (later, a serialised-`DbtGraph` dump) and ships artefacts in the image.
  Pro: no Cosmos code change today; reuses an Astronomer pathway. Con:
  Astronomer-only; can't carry serialised-`DbtGraph` until Cosmos defines
  the format. Verdict: candidate for sub-ticket #1 (docs + recipe).
- **`AstroBundleBackend` wrapped as `DbtDagBundle`.** Cosmos-owned
  `DagBundle` subclass bundling DAG files + preprocessed artefacts via
  Airflow 3's `DagBundleModel` — manifest + `dbt_packages/` + partial_parse
  today, plus serialised `DbtGraph` once defined. Pro: OSS-friendly;
  `DagBundleModel` already imported in `tests/utils.py:54-67`; decoupled
  from Astro CLI; natural home for serialised-`DbtGraph`. Con: Airflow 3
  only (Cosmos still supports 2.x); new public API surface; bundle
  versioning + refresh semantics need design. Verdict: candidate for
  sub-ticket #3, only if numbers justify.
- **Monkey-patch DAG-processor caching.** At Cosmos import, patch
  Airflow's DAG processor so re-parses of unchanged files skip
  `DbtGraph.load()` and reuse a memoised graph. Pro: no API change;
  potentially fast low-touch win. Con: fragile across Airflow versions;
  duplicates `cosmos/cache.py`'s purpose with a hidden second layer
  likely to surface as "stale DAG" bugs; version-pinned patches + CI
  matrix expansion. Verdict: **rejected** unless measurement shows
  `DBT_LS_CACHE` + `DBT_MANIFEST` still leave a measurable gap.

## Sub-tickets (preliminary)

PR-ready once numbers are in; update placeholders before posting.

1. **Document deploy-time preprocessing + Astro CLI recipe.** Artefact:
   dbt graph (dbt-side). Gate: unconditional — lowest-risk OSS-compatible
   win. Worked example in `docs/optimize_performance/deploy_time_preprocessing.rst`
   showing `dbt deps && dbt parse` in an Astro CLI hook, shipping
   `target/manifest.json` + `dbt_packages/`, with
   `ProjectConfig(manifest_path=..., install_dbt_deps=False)` and
   `RenderConfig(load_method=LoadMode.DBT_MANIFEST)`. Validate against
   `fhir-dbt-analytics`; cite parse-time reduction (TBD). Caveat: users on
   `LoadMode.DBT_LS_CACHE` already get most of the dbt-side benefit on
   warm cache — the win shows up on cold deploys and frequent misses.
2. **(Conditional) Prototype serialised `DbtGraph` LoadMode.** Artefact:
   dbt graph (Cosmos-side floor). Gate: only if measurement 2 shows
   non-trivial residual `DbtGraph.load()` time under `LoadMode.DBT_MANIFEST`.
   Add `LoadMode.DBT_GRAPH_PICKLE` (or similar) that pickles
   `DbtGraph.nodes`, `tests_per_model`, `filtered_nodes`, `load_method` at
   deploy and unpickles at parse, skipping the manifest walk. Versioned
   format validated against a dbt-project hash to prevent stale loads.
3. **(Conditional) Provide a `DbtDagBundle` for Airflow 3.** Artefact: all
   three (+ serialised `DbtGraph` if #2 lands). Gate: after #1 lands and
   numbers justify the API surface. Cosmos-owned `DagBundleModel` subclass
   shipping artefacts alongside DAG files, with versioning, refresh
   semantics, and reuse of `cosmos/cache.py` for staging. Airflow 3 only;
   feature-flag while 2.x is still supported. Also covers `dbt_packages`
   for users who don't control their image.
4. **(Conditional) Pre-stage `partial_parse.msgpack` for `DBT_LS` users.**
   Artefact: partial_parse. Gate: only if measurement 4 under
   `LoadMode.DBT_LS` shows a non-trivial delta — under `DBT_MANIFEST` the
   file has no parse-time effect. Either extend `cosmos/cache.py` to stage
   a shipped file automatically, or document the pattern. Default: docs
   only; promote to code if the delta is large.
5. **(Rejected unless numbers justify) Monkey-patch DAG processor.**
   Artefact: dbt graph (Cosmos-side). Gate: only if #1–#4 plus
   `LoadMode.DBT_LS_CACHE` still leave a measurable gap. Patch Airflow's
   DAG-processor caching to skip `DbtGraph.load()` on unchanged DAG
   files; document the staleness contract explicitly; pin to a specific
   Airflow version range in CI.

## Code-location references

Verify these still exist before treating any recommendation as load-bearing.

- **Constants/enums:** `cosmos/constants.py:72-82` (`LoadMode`),
  `cosmos/constants.py:98-113` (`ExecutionMode`), `cosmos/constants.py:23`
  (`DBT_PARTIAL_PARSE_FILE_NAME`).
- **Parse + graph path:** `cosmos/converter.py:320-347` (graph load + timing
  log), `cosmos/dbt/graph.py:418` (`DbtGraph`), `cosmos/dbt/graph.py`
  (`DbtGraph.load()`, `DbtGraph.run_dbt_deps()`,
  `load_via_dbt_ls_without_cache()`), `cosmos/cache.py` (caching layer),
  `tests/utils.py:54-67` (existing `DagBundleModel` usage).
- **Related docs:** `docs/faq.rst`,
  `docs/optimize_performance/optimize_rendering.rst`,
  `docs/optimize_performance/caching.rst`.
- **Upstream benchmark harness:** `github.com/astronomer/cosmos-benchmark` —
  `benchmark/Dockerfile`, `benchmark/dags/cosmos_dags.py`,
  `benchmark/run-complex-test.sh`,
  `benchmark/post-process/report-dag-run-pool-metrics.sh`.
