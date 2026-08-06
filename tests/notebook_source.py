"""Load code out of the Quality Catalog notebooks.

The notebooks are the only copy of the engine, so the tests read them the way
Fabric does: `%run` flattens every referenced notebook into one namespace, and
the entry-point cell at the bottom is what a human presses Run on.

This loader reproduces that. It executes a notebook's code cells into a module
namespace, skipping:

  * `%run` cells — composition is the caller's job here, exactly as it is in
    Fabric, so a test can compose QC_Engine + QC_Preflight itself;
  * cells tagged ``entrypoint`` — those *do* the work (create tables, run the
    catalog) and must not fire on import.
"""

from __future__ import annotations

import json
import sys
import types
from functools import lru_cache
from pathlib import Path

NOTEBOOK_DIR = Path(__file__).resolve().parent.parent / "notebooks"


def notebook_path(name: str) -> Path:
    return NOTEBOOK_DIR / f"{name}.ipynb"


def notebook_cells(name: str) -> list[dict]:
    return json.loads(notebook_path(name).read_text(encoding="utf-8"))["cells"]


def is_run_magic(source: str) -> bool:
    return source.lstrip().startswith("%run")


def is_entrypoint(cell: dict) -> bool:
    return "entrypoint" in cell.get("metadata", {}).get("tags", [])


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
