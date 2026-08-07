# Deployment Runbook

This repository holds the Quality Catalog as six Fabric PySpark notebooks, one
directory each under `notebooks/`. It carries no Lakehouse Files: Fabric
deployment pipelines do not promote the `Files` section of a lakehouse, so
anything that lived there could not be moved from dev to test to production.
Notebooks are first-class deployable items, so everything — engine, config, and
the rule catalogs — lives in a notebook.

**A file is a cell.** Each notebook's directory holds one `.py` file per cell,
named `01_...`, `02_...` and so on — that prefix is the paste order. A file
holds nothing but the cell's own content: no header, no `# CELL` marker, no
`# META` block, nothing to skip and nothing to stop before. Deploying a
notebook is: for each file, in order, add a cell and paste the file in, whole.

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

## Creating a notebook from a directory here

For each of the six directories in `notebooks/`:

1. Create a notebook in the workspace and name it **exactly** after the
   directory — `notebooks/QC_Config/` becomes a notebook called `QC_Config`.
   The names have to match: `%run QC_Config` resolves by display name, and a
   typo there fails at run time, not at save time.
2. List the directory's files, sorted by name — that sort order is the paste
   order, which is why every file starts with a zero-padded number.
3. For each file, add a cell in the notebook and paste the whole file into it.

So `notebooks/QC_Preflight/` reads:

```
01_run_QC_Config.py   ->  cell 1  ->  %run QC_Config
02_run_QC_Rules.py     ->  cell 2  ->  %run QC_Rules
03_run_QC_Engine.py    ->  cell 3  ->  %run QC_Engine
04_definitions.py      ->  cell 4  ->  the body
05_entrypoint.py       ->  cell 5  ->  the entry point
```

Five files, five cells, no assembly required — the directory listing is the
deployment plan.

**One `%run` per cell, and nothing else in it.** Not two `%run` lines together,
not a `%run` with a comment above it. Fabric refuses both:

```
MagicUsageError: %run cannot run with other code or magic commands.
```

This can only happen now by pasting more than one file into the same cell —
each `%run` file holds exactly one line, so a correct one-file-per-cell paste
cannot trigger it. If you hit it, check that you didn't merge two files, or
paste a file's content on top of an existing cell instead of a new one.

Notes:

- The last file in `QC_Setup_Tables/`, `QC_Preflight/` and
  `QC_Run_Validation/` starts with `# ENTRYPOINT`. That is the cell that does
  the work; everything above it only defines things.
- `QC_Config/`, `QC_Rules/` and `QC_Engine/` hold one file each — one cell,
  one paste.

## First deployment

1. Create all six notebooks in the **development** workspace, as above.
2. Attach the default lakehouse to `QC_Setup_Tables`, `QC_Preflight` and
   `QC_Run_Validation`. The engine reads source tables through the Spark
   metastore, so the run resolves against whichever lakehouse is attached.
3. Run `QC_Setup_Tables` once to create the output tables.
4. Run `QC_Preflight` and confirm it passes.
5. Run `QC_Run_Validation` and check the printed evidence.
6. Add the six notebooks to the deployment pipeline and promote to test, then
   production. Only development needs the pasting — the pipeline carries the
   notebooks from there.

The default lakehouse binding is per-workspace and is *not* carried in the
notebook content. Set it in the target stage with a deployment rule, or rely on
lakehouse auto-binding if the target workspace has an equivalent lakehouse.
After promoting to a new stage, run `QC_Setup_Tables` there once.

## Promoting a change

1. Edit the file here and paste it over the matching cell in the development
   workspace, or edit it in Fabric and paste it back into the file. Only the
   changed cell moves — that is what one file per cell buys: the diff names
   the file, and the file names the cell.
2. Run `QC_Preflight` — it resolves every `where:`, `when:` and `check:`
   predicate against the real table schemas.
3. Promote through the deployment pipeline.
4. Run `QC_Setup_Tables` in the target stage if the engine's output schemas
   changed.

Keep `notebooks/` in this repository in step with the workspace, in both
directions. The pytest suite runs against these files, so a change made only in
Fabric is a change nothing tests.

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
