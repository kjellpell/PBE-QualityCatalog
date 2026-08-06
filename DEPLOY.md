# Deployment Runbook

This repository holds the Quality Catalog as six Fabric PySpark notebooks. It
carries no Lakehouse Files: Fabric deployment pipelines do not promote the
`Files` section of a lakehouse, so anything that lived there could not be moved
from dev to test to production. Notebooks are first-class deployable items, so
everything — engine, config, and the rule catalogs — lives in a notebook.

## The notebooks

| Notebook | Kind | Purpose |
|---|---|---|
| `QC_Config` | library | `QUALITY_CATALOG_CONFIG` and `QUALITY_CATALOG_RUNTIME` |
| `QC_Rules` | library | `RULE_CATALOG_SOURCES` — one cell per rule catalog |
| `QC_Engine` | library | the validation engine |
| `QC_Setup_Tables` | entry point | creates the three Delta output tables |
| `QC_Preflight` | entry point | validates the catalogs before a run is scheduled |
| `QC_Run_Validation` | entry point | runs the catalog; schedule this one |

Library notebooks only define things. Entry-point notebooks pull them in with
`%run`, which executes the referenced notebook in the *same* session so its
functions and values land in the caller's namespace. The last cell of each
entry-point notebook is the one that does the work.

`%run` resolves a notebook by display name within the same workspace, so the
references keep working in every stage without rewriting.

## First deployment

1. Import all six `.ipynb` files from `notebooks/` into the **development**
   workspace (Data Engineering → Import notebook → From this computer). Keep
   the file names: `%run QC_Config` looks the notebook up by that name.
2. Attach the default lakehouse to `QC_Setup_Tables`, `QC_Preflight` and
   `QC_Run_Validation`. The engine reads source tables through the Spark
   metastore, so the run resolves against whichever lakehouse is attached.
3. Run `QC_Setup_Tables` once to create the output tables.
4. Run `QC_Preflight` and confirm it passes.
5. Run `QC_Run_Validation` and check the printed evidence.
6. Add the six notebooks to the deployment pipeline and promote to test, then
   production.

The default lakehouse binding is per-workspace and is *not* carried in the
notebook content. Set it in the target stage with a deployment rule, or rely on
lakehouse auto-binding if the target workspace has an equivalent lakehouse.
After promoting to a new stage, run `QC_Setup_Tables` there once.

## Promoting a change

1. Edit the notebook in the development workspace (or edit the `.ipynb` here
   and re-import).
2. Run `QC_Preflight` — it resolves every `where:`, `when:` and `check:`
   predicate against the real table schemas.
3. Promote through the deployment pipeline.
4. Run `QC_Setup_Tables` in the target stage if the engine's output schemas
   changed.

Keep `notebooks/` in this repository in step with the workspace. The pytest
suite runs against these files, so a change made only in Fabric is a change
nothing tests.

## Changing configuration

`QC_Config` holds every setting:

- `DEFAULT_SCHEMA` and the three output table names.
- `FAIL_ON_EMPTY_SOURCE`
- `MAX_RULE_RETRIES`
- `RULE_TIMEOUT_SECONDS`
- `RETRYABLE_ERROR_MARKERS`
- `CATALOG_FILTER_OVERRIDES`

The values are identical in every stage, so `QC_Config` is promoted unchanged.
If a setting ever has to differ per stage, that is a change to `QC_Config`, not
to the engine.

## Validation checklist

1. All six notebooks exist in the workspace, under their original names.
2. A default lakehouse is attached to the three entry-point notebooks.
3. `QC_Setup_Tables` has been run in this workspace.
4. `QC_Preflight` passes.
5. One `QC_Run_Validation` run completes, and `dq_run_results`,
   `dq_violations` and `dq_execution_metrics` hold rows for it.
