# Deploy-time preprocessing PoC — scoping doc

**Status:** investigation in progress. Benchmark numbers pending — see the
"Open benchmark work" section.

This is an engineering scoping doc, not user-facing documentation. It lives
outside the Sphinx toctree on purpose. The user-facing matrix is at
`docs/optimize_performance/deploy_time_preprocessing.rst`.

## Why this PoC exists

Cosmos may (re)compute three dbt artefacts on every DAG parse or every task
start:

| Artefact | What it is | Where it's computed today |
|---|---|---|
| **dbt graph** | The Cosmos `DbtGraph` object (nodes + filtered_nodes), defined in `cosmos/dbt/graph.py:418` | `DbtGraph.load()` at DAG-parse time, called from `cosmos/converter.py:320-331`. Rebuilt on every parse regardless of `LoadMode`. |
| **dbt_packages/** | dbt package install dir | `DbtGraph.run_dbt_deps()` (`cosmos/dbt/graph.py:824`) at parse time when `install_dbt_deps=True`; per-task when operator `install_deps=True` |
| **partial_parse.msgpack** | dbt's own parse cache | dbt-internal; staged into temp run dirs by Cosmos at `cosmos/dbt/graph.py:912-920`; cached at `cosmos/cache.py:173-214` |

**Critical distinction for the dbt graph artefact:** the `DbtGraph` Cosmos
object is *not* the same thing as `manifest.json`. `LoadMode.DBT_MANIFEST`
precomputes the dbt-side *input* (manifest.json) but Cosmos still walks
that input on every parse to construct the `DbtGraph`. The PoC's
hypothesis is that the residual `DbtGraph` construction cost — the
Cosmos-side work between reading the manifest and emitting Airflow tasks
— is worth eliminating by shipping a serialised `DbtGraph` directly.

Production deployments typically have multiple workers and many DAGs. Each
of these artefacts has a slightly different cost surface, and the
combination of `LoadMode` and `ExecutionMode` determines whether
precomputing it actually helps. The PoC's first deliverable is to measure
the headline cases and decide which ones are worth automating.

## Methodology

Time is recorded for a single LOCAL DAG (`example_dbt_dag`) against the
`fhir-dbt-analytics` project (185 models) in the `cosmos-benchmark`
harness. The matrix in the user-facing doc is reasoned from the code, not
measured cell-by-cell.

Two surfaces matter:

- **Parse-time** — measured via `cosmos/converter.py:341`'s
  `It took %.3gs to parse the dbt project for DAG using ...` log. Extract
  from the DAG-processor pod's logs.
- **DAG runtime + worker CPU/memory** — already collected by
  `cosmos-benchmark`'s `run-complex-test.sh` →
  `post-process/report-dag-run-pool-metrics.sh`.

The `converter.py:341` log measures total `DbtGraph.load()` wall time. To
isolate the Cosmos-side construction cost from the dbt-side input cost,
we measure two pairs:

1. **dbt graph — dbt-side cost (today's available win).** Baseline runs
   `dbt ls` at parse (`LoadMode.DBT_LS`); precomputed variant has
   `RUN dbt parse` in the Dockerfile and switches the DAG to
   `LoadMode.DBT_MANIFEST`. Delta = dbt-invocation cost saved.
2. **dbt graph — Cosmos-side construction floor.** With `LoadMode.DBT_MANIFEST`
   in place, the `converter.py:341` time is the residual cost of building
   the `DbtGraph` from the prebuilt manifest. This is the floor a future
   serialised-`DbtGraph` mechanism would aim to push toward zero. We do
   not have a code-side prototype yet; the number tells us whether it's
   worth building one.
3. **dbt_packages.** Baseline pre-installs via `RUN dbt deps` (already
   present in the upstream Dockerfile); without-precomputation variant
   removes that line and sets `install_dbt_deps=True`.
4. **partial_parse.** Baseline keeps `target/partial_parse.msgpack`
   alongside the manifest; without-precomputation variant adds a final
   `RUN rm -f target/partial_parse.msgpack`. If signal is zero under
   `LoadMode.DBT_MANIFEST` (expected, since dbt isn't invoked at parse),
   re-run the toggle under `LoadMode.DBT_LS` to isolate the cold-deploy
   effect.

## Open benchmark work

These are the concrete changes needed in `astronomer/cosmos-benchmark` to
produce the numbers. They are *not* applied yet — they require BigQuery
credentials and a local kind cluster (`~10 vCPU / 20 GiB`) the author
doesn't have set up.

### Change 1 — `benchmark/Dockerfile`: build-arg toggles

Replace the existing fixed `RUN dbt deps` line with build-arg-gated steps:

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

Build the six images with distinct tags, e.g.:

```bash
docker build --build-arg PRECOMPUTE_DEPS=0 -t cosmos-bench:no-deps .
docker build --build-arg PRECOMPUTE_DEPS=1 -t cosmos-bench:with-deps .          # current upstream baseline
docker build --build-arg PRECOMPUTE_MANIFEST=1 -t cosmos-bench:with-manifest .
docker build --build-arg PRECOMPUTE_MANIFEST=0 -t cosmos-bench:no-manifest .    # same as with-deps above
docker build --build-arg PRECOMPUTE_MANIFEST=1 --build-arg PRECOMPUTE_PARTIAL_PARSE=1 -t cosmos-bench:with-pp .
docker build --build-arg PRECOMPUTE_MANIFEST=1 --build-arg PRECOMPUTE_PARTIAL_PARSE=0 -t cosmos-bench:no-pp .
```

Note: this measures the **dbt-side** component of the dbt-graph artefact
(manifest precomputation). The **Cosmos-side** component (serialised
`DbtGraph`) is not measured here because no such mechanism exists yet —
the `LoadMode.DBT_MANIFEST` parse-time number gives us the floor that a
serialised-`DbtGraph` mechanism would need to beat to be worth building.

### Change 2 — `benchmark/dags/cosmos_dags.py`: manifest variant

The existing DAG uses `install_dbt_deps=False` and the default LoadMode
(AUTOMATIC, no manifest_path → falls back to DBT_LS). Make it
env-toggleable so the same DAG file can drive both the with-manifest and
without-manifest runs:

```python
import os
from pathlib import Path
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = Path(__file__).parent.parent
USE_MANIFEST = os.getenv("COSMOS_BENCH_USE_MANIFEST", "0") == "1"

manifest_path = (DBT_PROJECT_PATH / "target" / "manifest.json") if USE_MANIFEST else None
load_method = LoadMode.DBT_MANIFEST if USE_MANIFEST else LoadMode.DBT_LS

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    install_dbt_deps=os.getenv("COSMOS_BENCH_INSTALL_DEPS", "0") == "1",
    manifest_path=manifest_path,
)

# in DbtDag(...):
render_config=RenderConfig(test_behavior=TestBehavior.NONE, load_method=load_method),
```

### Change 3 — `benchmark/post-process/report-parse-time.sh`: new script

A thin extractor that mirrors `report-dag-run-pool-metrics.sh`'s shape:

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
  | tail -1   # last parse in the window
```

`run-complex-test.sh` then appends the result to `$METRICS_CSV`. New
columns: `parse_time_s, load_mode`.

### Change 4 — `benchmark/results/preprocessing-poc.csv`: committed numbers

After the six runs, commit the CSV in-repo so it stays linked from this
scoping doc.

## Per-artefact recommendation (preliminary)

To be finalised once numbers land. Preliminary read from code:

### dbt graph (the `DbtGraph` object)

- **Today's mechanism (`LoadMode.DBT_MANIFEST`)** is the largest piece of
  the available win and is already supported by Cosmos. Worth automating
  the *deploy step* (CI hook / Dockerfile / DAG bundle) so users don't
  have to wire it themselves.
- **Serialising the `DbtGraph` itself** is the deeper opportunity. If the
  Step-2 measurement shows non-trivial residual cost in `DbtGraph.load()`
  under `LoadMode.DBT_MANIFEST` (i.e. the floor is far from zero), it's
  worth prototyping a new `LoadMode` (`LoadMode.DBT_GRAPH_PICKLE` or
  similar) that pickles `DbtGraph.nodes` / `tests_per_model` /
  `filtered_nodes` at deploy time and unpickles at parse. Skip if the
  floor is already near-zero.
- **Caveat for both:** users on `LoadMode.DBT_LS_CACHE` already get most
  of the dbt-side benefit on warm cache. The win shows up on cold deploys
  and frequent cache misses.

### dbt_packages

- **Worth automating only when ``install_dbt_deps=True`` or operators have
  ``install_deps=True``.** The current cosmos-benchmark baseline already
  does this preprocessing (image-time `dbt deps`). For users who don't
  control their image, packaging this as a `DbtDagBundle` would matter.

### partial_parse

- **Likely documentation only.** Under `LoadMode.DBT_MANIFEST` (the
  recommended path), the partial_parse file has no parse-time effect.
  Under `LoadMode.DBT_LS` it does, but those users should arguably move
  to `DBT_MANIFEST` instead. Keep as opportunistic, not automated.

## Evaluation of automation paths

Three approaches called out in the originating ticket. Each evaluated
against: (a) what existing Cosmos/Airflow surface it touches, (b) blast
radius and risk, (c) maintenance burden. Note that these are the
*mechanisms* for shipping any of the three artefacts; the per-artefact
recommendation above decides *which* artefacts to ship.

### Otto / Astro CLI deploy hook

- **What it is:** Use the Astro CLI's existing build-time hook surface to
  run `dbt deps && dbt parse` (and optionally a future serialised-`DbtGraph`
  dump) at deploy time, then ship the artefacts in the image.
- **Pros:** No Cosmos code change required for today's mechanism. Reuses
  an already-supported Astronomer pathway. Users already familiar with
  Astro CLI workflows inherit it cleanly.
- **Cons:** Tied to Astronomer's CLI; OSS users on plain Airflow get no
  benefit. Documentation-only contribution from the Cosmos side. Cannot
  carry the serialised-`DbtGraph` artefact until Cosmos defines that
  format.
- **Cosmos changes:** none required for today's manifest-based path; just
  a docs note. For serialised-`DbtGraph`, Cosmos needs to ship the
  serialisation format first.
- **Verdict:** Candidate for sub-ticket #1 (documentation + reference
  recipe).

### `AstroBundleBackend` wrapped as `DbtDagBundle`

- **What it is:** Provide a Cosmos-owned `DagBundle` subclass that bundles
  DAG files together with their preprocessed dbt artefacts, leveraging
  Airflow 3's `DagBundleModel`. Could ship manifest + `dbt_packages/` +
  partial_parse today, and a serialised `DbtGraph` once that format
  exists.
- **Pros:** OSS-friendly; works for any Airflow 3 user, not just Astro.
  Cosmos already imports `DagBundleModel` in `tests/utils.py:54-67` for
  test infrastructure, so the dependency is already in scope.
  Decouples preprocessing from the Astro CLI specifically. Natural home
  for a future serialised-`DbtGraph` artefact.
- **Cons:** Airflow 3 only (Cosmos still supports 2.x). Requires Cosmos
  to own a new public API surface. Bundle versioning and refresh
  semantics need design.
- **Cosmos changes:** new module (`cosmos/bundles/`?), new tests, docs.
- **Verdict:** Candidate for sub-ticket #3 (mechanism), opened *only* if
  the headline benchmark numbers justify the surface area.

### Monkey-patch DAG-processor caching

- **What it is:** At Cosmos import time, patch Airflow's DAG processor so
  that re-parses of an unchanged DAG file skip the Cosmos `DbtGraph.load()`
  call and reuse a memoised `DbtGraph` from the previous parse in the same
  process. This is effectively an in-process cache of the constructed
  `DbtGraph` object.
- **Pros:** Avoids any user-facing API change. Could be a fast, low-touch
  win for the `DbtGraph` construction cost specifically.
- **Cons:** Monkey-patching Airflow internals is fragile across Airflow
  versions and intersects with Airflow's own DAG parsing model in
  non-obvious ways. Cosmos already has a deliberate caching layer
  (`cosmos/cache.py`) for the same purpose; adding a second, hidden
  caching layer at the Airflow level risks divergence and is likely to
  surface as "stale DAG" bugs after deploys.
- **Cosmos changes:** Airflow-version-pinned patches; CI matrix expansion.
- **Verdict:** Recommend **rejected** unless the Step-2 measurement shows
  that `LoadMode.DBT_LS_CACHE` and `LoadMode.DBT_MANIFEST` together still
  leave a measurable parse-time gap that justifies the risk. Document the
  reasoning in the sub-ticket either way.

## Draft sub-tickets

The titles and one-paragraph descriptions below are PR-ready once the
numbers are in. Update the numeric placeholders before posting.

### 1. Document deploy-time preprocessing pattern and Astro CLI recipe

Add a worked example to `docs/optimize_performance/deploy_time_preprocessing.rst`
showing how to run `dbt deps && dbt parse` as part of an Astro CLI deploy
hook, ship the resulting `target/manifest.json` and `dbt_packages/`, and
configure `ProjectConfig(manifest_path=..., install_dbt_deps=False)` with
`RenderConfig(load_method=LoadMode.DBT_MANIFEST)`. Validate against
`fhir-dbt-analytics` in `cosmos-benchmark`; cite the dbt-side parse-time
reduction (TBD). Lowest-risk, OSS-compatible win. Sub-ticket #1 covers
the *dbt-side* portion of the dbt-graph artefact only.

### 2. (Conditional) Prototype serialised `DbtGraph` LoadMode

Only if the Step-2 measurement shows non-trivial residual `DbtGraph.load()`
time under `LoadMode.DBT_MANIFEST`. Add a new `LoadMode.DBT_GRAPH_PICKLE`
(or similar) that pickles `DbtGraph.nodes`, `tests_per_model`,
`filtered_nodes`, and `load_method` at deploy time and unpickles at
parse — skipping the manifest walk entirely. The shipped artefact format
should be versioned and validated against the dbt-project hash so a stale
graph can't be silently loaded after the project changes. Skip if the
residual cost is already near-zero.

### 3. (Conditional) Provide a `DbtDagBundle` for Airflow 3 DAG bundles

Implement a Cosmos-owned subclass of Airflow 3's `DagBundleModel` that
ships dbt artefacts (`manifest.json`, `dbt_packages/`, optionally
`partial_parse.msgpack`, and the serialised `DbtGraph` from sub-ticket #2
if it lands) alongside DAG files. Includes versioning, refresh
semantics, and reuse of the existing `cosmos/cache.py` for the
artefact-staging step. Only open after sub-ticket #1 lands and benchmark
numbers justify the API surface. Airflow 3 only; gate behind a feature
flag while we still support 2.x.

### 4. (Conditional) Pre-stage `partial_parse.msgpack` for `DBT_LS` users

Only if the Step-4 numbers show a non-trivial parse-time delta under
`LoadMode.DBT_LS` after shipping `partial_parse.msgpack`. Either extend
`cosmos/cache.py` to stage a shipped partial_parse into the temp run
directory automatically, or document the pattern. Default: documentation
only; promote to code if the delta is large.

### 5. (Rejected unless numbers justify) Monkey-patch DAG processor

Only open if all of the above plus `LoadMode.DBT_LS_CACHE` still leave a
measurable parse-time gap. Patch Airflow's DAG-processor caching to skip
`DbtGraph.load()` on unchanged DAG files. Document the staleness contract
explicitly. Pin to a specific Airflow version range in CI.

## Code-location references

For future readers — these are the locations the user-facing doc and this
scoping doc are derived from. Verify they still exist before treating any
specific recommendation as load-bearing:

- `cosmos/constants.py:72-82` — `LoadMode` enum
- `cosmos/constants.py:98-113` — `ExecutionMode` enum
- `cosmos/constants.py:23` — `DBT_PARTIAL_PARSE_FILE_NAME`
- `cosmos/converter.py:320-347` — graph load + timing log
- `cosmos/dbt/graph.py:418` — `DbtGraph` class (the Cosmos-side dbt graph)
- `cosmos/dbt/graph.py` `DbtGraph.load()` — dispatch by LoadMode
- `cosmos/dbt/graph.py` `DbtGraph.run_dbt_deps()` — dbt deps runner
- `cosmos/dbt/graph.py` `load_via_dbt_ls_without_cache()` — DBT_LS path
- `cosmos/cache.py` — Cosmos-side caching layer
- `tests/utils.py:54-67` — existing `DagBundleModel` usage in tests
- `docs/faq.rst` — existing deploy-time-preprocessing recommendation
- `docs/optimize_performance/optimize_rendering.rst` — parse-time knobs
- `docs/optimize_performance/caching.rst` — caching layer details

Upstream benchmark harness:

- `github.com/astronomer/cosmos-benchmark` — `benchmark/Dockerfile`,
  `benchmark/dags/cosmos_dags.py`, `benchmark/run-complex-test.sh`,
  `benchmark/post-process/report-dag-run-pool-metrics.sh`.
