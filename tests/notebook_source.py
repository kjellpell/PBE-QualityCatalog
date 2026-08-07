"""Load code out of the Quality Catalog notebooks.

The notebooks are the only copy of the engine, so the tests read them the way
Fabric does: `%run` flattens every referenced notebook into one namespace, and
the entry-point cell at the bottom is what a human presses Run on.

This loader reproduces that. It executes a notebook's code cells into a module
namespace, skipping:

  * `%run` cells — composition is the caller's job here, exactly as it is in
    Fabric, so a test can compose QC_Engine + QC_Preflight itself;
  * the cell marked ``# ENTRYPOINT`` — that one *does* the work (creates
    tables, runs the catalog) and must not fire on import.

A notebook is a directory and **a cell is a file**, ordered by the ``NN_``
prefix on the filename. These notebooks are created in the Fabric UI and filled
in by pasting cells, so the layout exists to make that paste impossible to get
wrong: one file goes into one cell, whole, with nothing to split and nothing to
skip. An earlier layout kept each notebook in a single file with
``# CELL ********************`` comments between the cells, and it failed in the
obvious way — handed one file, you paste one file, and a ``%run`` sharing a cell
with anything else dies with ``MagicUsageError``. Splitting is the filesystem's
job now, not the reader's.
"""

from __future__ import annotations

import sys
import types
from functools import lru_cache
from pathlib import Path

NOTEBOOK_DIR = Path(__file__).resolve().parent.parent / "notebooks"


def notebook_path(name: str) -> Path:
    """The notebook's directory. One directory per notebook, one file per cell."""
    return NOTEBOOK_DIR / name


def cell_paths(name: str) -> list[Path]:
    """Every cell file, in the order Fabric will hold them.

    Sorted by filename, which is why the ``NN_`` prefix is zero-padded: it is
    the cell order, and ``10_`` must not sort before ``2_``.
    """
    return sorted(notebook_path(name).glob("*.py"))


def notebook_cells(name: str) -> list[dict]:
    """Cells in notebook order, shaped like the nbformat dicts this once read.

    Verbatim: a file *is* a cell, so nothing is stripped, joined or split.
    """
    return [
        {"cell_type": "code", "source": path.read_text(encoding="utf-8")}
        for path in cell_paths(name)
    ]


def is_run_magic(source: str) -> bool:
    return source.lstrip().startswith("%run")


def is_entrypoint(cell: dict) -> bool:
    """Marked by a comment, not by cell metadata.

    Metadata does not survive a copy-paste into Fabric, and a marker that only
    ever exists in the repository is a marker that tells the person deploying
    nothing. This one names the cell that actually does something.
    """
    body = "".join(cell["source"]).strip()
    return bool(body) and body.splitlines()[0].startswith("# ENTRYPOINT")


def executable_cells(name: str, include_entrypoint: bool = False) -> list[tuple[int, str]]:
    """``(cell number, source)`` for each code cell the tests should run.

    Cell numbers are 1-based over *all* cells, so they match both what the
    notebook shows and the ``NN_`` prefix on the file.
    """
    cells = []
    for number, cell in enumerate(notebook_cells(name), start=1):
        if cell["cell_type"] != "code":
            continue
        source = "".join(cell["source"])
        if is_run_magic(source):
            continue
        if is_entrypoint(cell) and not include_entrypoint:
            continue
        cells.append((number, source.rstrip("\n")))
    return cells


def cell_filename(name: str, number: int) -> str:
    """The path of the cell's file, so a traceback names something openable."""
    return str(cell_paths(name)[number - 1].relative_to(NOTEBOOK_DIR.parent))


def notebook_code(name: str, include_entrypoint: bool = False) -> str:
    """Every executable cell as one source string, for whole-notebook analysis."""
    return "\n\n".join(source for _, source in executable_cells(name, include_entrypoint)) + "\n"


def load_notebook(name: str, into: types.ModuleType | None = None) -> types.ModuleType:
    """Exec a notebook into a module namespace, or into an existing one.

    Passing ``into`` composes notebooks in a single namespace, which is what
    `%run` does in Fabric — QC_Preflight's functions expect QC_Engine's names
    to already be there.

    Cells are compiled one at a time, under the filename of the cell's file.
    That matches Fabric — each cell is its own compilation unit, so a
    ``from __future__`` import is scoped to its cell in both places — and it
    means a traceback points at a file somebody can open, rather than at a line
    offset into a concatenation that exists nowhere.
    """
    module = into if into is not None else types.ModuleType(name)
    # @dataclass resolves its annotations through sys.modules[cls.__module__].
    # In Fabric the notebook namespace is __main__, which is always registered;
    # here it has to be put there deliberately.
    sys.modules.setdefault(module.__name__, module)
    for number, source in executable_cells(name):
        exec(compile(source, cell_filename(name, number), "exec"), module.__dict__)
    return module


@lru_cache(maxsize=None)
def engine_namespace() -> types.ModuleType:
    """QC_Engine, loaded once per test session.

    Safe to call at module import time: the library notebooks define names and
    nothing else — no Spark session is opened until `configure()` is called.
    """
    return load_notebook("QC_Engine")
