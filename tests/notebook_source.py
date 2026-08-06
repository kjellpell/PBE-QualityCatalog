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

The notebooks are stored in Fabric's own notebook-source format: plain Python
with ``# CELL ********************`` between cells and a ``# META`` block after
each. That format exists to be read and pasted by a human — these notebooks are
created in the Fabric UI and filled in by hand — and parsing it here costs
about ten lines.
"""

from __future__ import annotations

import re
import sys
import types
from functools import lru_cache
from pathlib import Path

NOTEBOOK_DIR = Path(__file__).resolve().parent.parent / "notebooks"

# Anchored on the run of asterisks, not the bare word: cell prose can and does
# contain lines like "# CELL 6 — Main validation engine", and mistaking one for
# a delimiter would silently truncate a cell.
_CELL_DELIMITER = re.compile(r"^# CELL \*{4,}$", re.M)
_META_BLOCK = re.compile(r"^# METADATA \*{4,}$.*", re.M | re.S)


def notebook_path(name: str) -> Path:
    return NOTEBOOK_DIR / f"{name}.py"


def notebook_cells(name: str) -> list[dict]:
    """Cells in notebook order, shaped like the nbformat dicts this once read."""
    text = notebook_path(name).read_text(encoding="utf-8")
    return [
        {"cell_type": "code", "source": _META_BLOCK.sub("", chunk).strip("\n") + "\n"}
        for chunk in _CELL_DELIMITER.split(text)[1:]
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

    Cell numbers are 1-based over *all* cells, so they match what the notebook
    shows — they are what a traceback has to name to be worth reading.
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
    return f"{name}.ipynb[cell {number}]"


def notebook_code(name: str, include_entrypoint: bool = False) -> str:
    """Every executable cell as one source string, for whole-notebook analysis."""
    return "\n\n".join(source for _, source in executable_cells(name, include_entrypoint)) + "\n"


def load_notebook(name: str, into: types.ModuleType | None = None) -> types.ModuleType:
    """Exec a notebook into a module namespace, or into an existing one.

    Passing ``into`` composes notebooks in a single namespace, which is what
    `%run` does in Fabric — QC_Preflight's functions expect QC_Engine's names
    to already be there.

    Cells are compiled one at a time, under a filename naming the cell. That
    matches Fabric — each cell is its own compilation unit, so a
    ``from __future__`` import is scoped to its cell in both places — and it
    means a traceback points at a cell somebody can open, rather than at a line
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
