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


def notebook_code(name: str, include_entrypoint: bool = False) -> str:
    """The importable Python of a notebook, as one compilable source string."""
    blocks = []
    for cell in notebook_cells(name):
        if cell["cell_type"] != "code":
            continue
        source = "".join(cell["source"])
        if is_run_magic(source):
            continue
        if is_entrypoint(cell) and not include_entrypoint:
            continue
        blocks.append(source.rstrip("\n"))
    return "\n\n".join(blocks) + "\n"


def load_notebook(name: str, into: types.ModuleType | None = None) -> types.ModuleType:
    """Exec a notebook into a module namespace, or into an existing one.

    Passing ``into`` composes notebooks in a single namespace, which is what
    `%run` does in Fabric — QC_Preflight's functions expect QC_Engine's names
    to already be there.
    """
    module = into if into is not None else types.ModuleType(name)
    # @dataclass resolves its annotations through sys.modules[cls.__module__].
    # In Fabric the notebook namespace is __main__, which is always registered;
    # here it has to be put there deliberately.
    sys.modules.setdefault(module.__name__, module)
    exec(compile(notebook_code(name), f"{name}.ipynb", "exec"), module.__dict__)
    return module


@lru_cache(maxsize=None)
def engine_namespace() -> types.ModuleType:
    """QC_Engine, loaded once per test session.

    Safe to call at module import time: the library notebooks define names and
    nothing else — no Spark session is opened until `configure()` is called.
    """
    return load_notebook("QC_Engine")
