.. _deploy-time-preprocessing:

Deploy-time preprocessing
=========================

Cosmos depends on three dbt artefacts that can be expensive to (re)compute:
the Cosmos-side dbt graph (the ``DbtGraph`` object built in
``cosmos/converter.py``), installed dbt packages (``dbt_packages/``), and
dbt's partial-parse cache (``target/partial_parse.msgpack``). Each is
normally computed at DAG parse time or task execution time. By producing
them once at deploy time -- in CI/CD, in the Docker image, or as part of a
DAG bundle -- and shipping them alongside the DAGs, you remove that cost
from the hot path.

This page maps which combinations of :ref:`LoadMode <parsing-methods>` and
``ExecutionMode`` actually benefit from shipping each artefact, and which
do not. For the individual knobs themselves -- ``LoadMode.DBT_MANIFEST``,
``install_dbt_deps=False``, ``partial_parse=True`` -- see
:ref:`optimize-rendering` and :ref:`caching`.

The three artefacts
~~~~~~~~~~~~~~~~~~~

- **dbt graph** -- the Cosmos ``DbtGraph`` object, constructed in
  ``cosmos/converter.py`` on every DAG parse by calling
  ``DbtGraph.load()``. Today, no ``LoadMode`` precomputes the
  ``DbtGraph`` itself; every parse rebuilds it. The closest existing
  mechanism is ``LoadMode.DBT_MANIFEST``, which precomputes the dbt-side
  *input* (``manifest.json``) so dbt itself does not run at parse time --
  but Cosmos still walks the manifest and re-builds ``DbtGraph.nodes`` on
  each parse.
- **dbt_packages/** -- installed by ``DbtGraph.run_dbt_deps()`` when
  ``install_dbt_deps=True``; also re-installed per task when an operator
  has ``install_deps=True``.
- **partial_parse.msgpack** -- dbt's own internal parse cache. Used only
  when dbt itself is invoked (i.e. ``LoadMode.DBT_LS`` at parse, or any
  task that runs a dbt command).

When precomputation pays off
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two cost surfaces are affected by these artefacts:

- **Parse-time cost** -- paid by a single DAG-processor process every time
  Airflow re-parses your DAG file. Worker count is irrelevant; what matters
  is parse frequency and DAG count. Cosmos logs this at the ``INFO`` level
  on every parse:

  .. code-block:: text

     Cosmos performance (<cache_id>) - [<hostname>|<pid>]: It took 0.068s to parse the dbt project for DAG using LoadMode.DBT_LS_CACHE

- **Task-startup cost** -- paid by every worker (or every spawned
  pod / container) on every task that needs the artefact. This cost is
  multiplied by both worker count and task count, so it dominates in
  multi-worker setups with many models per DAG.

Pre-installing ``dbt_packages/`` mostly affects task-startup cost when
operators run with ``install_deps=True``. Pre-computing the dbt graph
mostly affects parse-time cost. ``partial_parse.msgpack`` affects both,
but only when dbt itself runs (parse time under ``DBT_LS``, task time on
every dbt invocation).

Parse-time applicability matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Whether shipping each artefact reduces DAG-parse time, by ``LoadMode``:

.. list-table::
   :header-rows: 1
   :widths: 22 18 18 18 24

   * - Artefact (parse time)
     - ``DBT_LS``
     - ``DBT_LS_CACHE``
     - ``DBT_LS_FILE``
     - ``DBT_MANIFEST``
   * - dbt graph
     - Partial -- shipping ``manifest.json`` + ``DBT_MANIFEST`` skips ``dbt ls`` but still rebuilds the ``DbtGraph`` per parse. Shipping a serialised ``DbtGraph`` itself would skip both. Not yet supported.
     - Partial -- the cache stores ``dbt ls`` output, not the ``DbtGraph``; the graph is reconstructed on each parse from cache. Same gap as above.
     - Partial -- this mode reads a prebuilt ``dbt ls`` JSON file; ``DbtGraph`` is still built per parse.
     - Partial -- this mode reads a prebuilt ``manifest.json``; ``DbtGraph`` is still built per parse.
   * - ``dbt_packages/``
     - Yes when ``install_dbt_deps=True`` -- skips ``dbt deps`` at parse.
     - Same as ``DBT_LS`` on cache miss; no effect on cache hit.
     - Yes when ``install_dbt_deps=True``.
     - No -- this mode does not invoke dbt at parse time.
   * - ``partial_parse.msgpack``
     - Yes -- cuts dbt's own parse cost when Cosmos runs ``dbt ls``.
     - Same as ``DBT_LS`` on cache miss; no effect on cache hit.
     - No -- this mode skips dbt invocation at parse.
     - No -- this mode skips dbt invocation at parse.

"Partial" means precomputing today reduces but does not eliminate the
cost. ``LoadMode.AUTOMATIC`` follows whichever underlying mode it
resolves to, typically ``DBT_MANIFEST`` if ``manifest_path`` is set,
otherwise ``DBT_LS``. ``LoadMode.CUSTOM`` is deprecated and not
recommended.

Task-startup applicability matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Whether shipping each artefact reduces task-startup cost, by ``ExecutionMode``:

.. list-table::
   :header-rows: 1
   :widths: 30 18 18 16 18

   * - Artefact (task time)
     - ``LOCAL`` / ``VIRTUALENV``
     - ``WATCHER``
     - ``KUBERNETES`` / cloud
     - ``AIRFLOW_ASYNC``
   * - dbt graph
     - No -- tasks consume dbt commands, not the ``DbtGraph``; the graph is a parse-time concept.
     - No.
     - No.
     - No.
   * - ``dbt_packages/``
     - Yes when ``install_deps=True`` -- avoids ``dbt deps`` per task.
     - Yes -- one producer task; pre-installed packages skip ``dbt deps`` once.
     - Yes only if baked into the task image; per-task install otherwise.
     - Yes -- the pre-compile step uses the shipped packages.
   * - ``partial_parse.msgpack``
     - Yes -- each dbt invocation reuses it instead of full reparse.
     - Yes -- the single producer task benefits once.
     - Yes only if baked into the task image.
     - Yes -- shared across the pre-compile + execute path.

"Cloud" here covers ``DOCKER``, ``AWS_EKS``, ``AWS_ECS``,
``AZURE_CONTAINER_INSTANCE``, and ``GCP_CLOUD_RUN_JOB`` -- they all run
tasks in containers with their own images, so the precomputed artefact
only helps when it is part of that image.

Where deploy-time preprocessing does not help
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **dbt graph today is only partially precomputable.** No ``LoadMode``
  ships a fully prebuilt ``DbtGraph``; every parse rebuilds it from
  whichever input is available. Shipping ``manifest.json`` is the
  largest portion of today's available win.
- **Cold cache only.** With ``LoadMode.DBT_LS_CACHE`` and an already-warm
  cache, the manifest-side and partial-parse benefits are already realised
  in the cache. Precomputation matters most on the first parse after a
  deploy.
- **Container-per-task modes without image baking.** ``KUBERNETES``,
  ``DOCKER``, and the cloud variants run tasks in separate images. An
  artefact in the DAG-processor image is not visible to the task
  container; bake it into the task image (or ship it via a shared
  volume) for benefit.
- **partial_parse + DBT_MANIFEST.** Under ``LoadMode.DBT_MANIFEST``,
  Cosmos does not invoke dbt at parse time, so a shipped
  ``partial_parse.msgpack`` has no parse-time effect. It still benefits
  task-time dbt invocations.
- **dbt_packages + container modes.** Pre-installing packages on the
  Airflow worker does not help a Kubernetes pod running a different
  image. The package install needs to happen inside the same image that
  the dbt command runs in.

How to produce the artefacts at deploy time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Today, only the dbt-side inputs to the dbt graph can be precomputed.
Run, in your CI / Dockerfile / pre-deploy step, from the dbt project root:

.. code-block:: bash

   dbt deps                # populates dbt_packages/
   dbt parse               # generates target/manifest.json and target/partial_parse.msgpack

Ship the resulting ``dbt_packages/`` and ``target/`` folders alongside your
DAGs. Then tell Cosmos to consume the prebuilt manifest instead of running
``dbt ls`` at parse:

.. code-block:: python

   from cosmos import DbtDag, ProjectConfig, RenderConfig
   from cosmos.constants import LoadMode

   DbtDag(
       dag_id="my_dbt_dag",
       project_config=ProjectConfig(
           dbt_project_path="/opt/airflow/dbt_project",
           manifest_path="/opt/airflow/dbt_project/target/manifest.json",
           install_dbt_deps=False,
           partial_parse=True,
       ),
       render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
       # ...
   )

This eliminates the dbt-side cost of building the dbt graph but does not
eliminate the Cosmos-side cost of constructing the ``DbtGraph`` object on
each parse. Fully precomputing the ``DbtGraph`` (so it can be deserialised
rather than rebuilt at parse) is being explored separately.

See :ref:`optimize-rendering` for the parse-time knobs in detail and
:ref:`caching` for the Cosmos caching layer that complements this pattern
when ``LoadMode.DBT_MANIFEST`` is not feasible.
