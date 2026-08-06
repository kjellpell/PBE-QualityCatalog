"""
Guards on the notebooks themselves.

The notebooks are the deployable artifact, so the things that break a Fabric
run — a cell that will not compile, a `%run` pointing at a notebook that was
renamed, two notebooks defining the same name into one flattened namespace —
have to fail here rather than in the workspace.
"""

import ast
import json

import pytest

from tests.notebook_source import (
    NOTEBOOK_DIR,
    cell_filename,
    executable_cells,
    is_entrypoint,
    is_run_magic,
    notebook_cells,
    notebook_code,
    notebook_path,
)

LIBRARY_NOTEBOOKS = ["QC_Config", "QC_Rules", "QC_Engine"]
ENTRY_NOTEBOOKS = ["QC_Setup_Tables", "QC_Preflight", "QC_Run_Validation"]
ALL_NOTEBOOKS = LIBRARY_NOTEBOOKS + ENTRY_NOTEBOOKS

# What each library notebook promises to the notebooks that %run it.
PUBLIC_NAMES = {
    "QC_Config": ["QUALITY_CATALOG_CONFIG", "QUALITY_CATALOG_RUNTIME"],
    "QC_Rules": ["RULE_CATALOG_SOURCES"],
    "QC_Engine": [
        "configure",
        "load_rule_catalogs",
        "run_quality_catalog",
        "run_with_metrics",
        "RESULT_SCHEMA",
        "VIOLATION_SCHEMA",
        "RULE_TYPES",
        "run_rule",
    ],
}


def _run_targets(name: str) -> list[str]:
    return [
        "".join(cell["source"]).strip().split()[1]
        for cell in notebook_cells(name)
        if cell["cell_type"] == "code" and is_run_magic("".join(cell["source"]))
    ]


def _top_level_names(name: str) -> set[str]:
    """Names a notebook binds at top level, excluding imports.

    Imports are excluded deliberately: two notebooks both importing
    SparkSession is not a conflict, two notebooks both defining `main` is.
    """
    names = set()
    for node in ast.parse(notebook_code(name, include_entrypoint=True)).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                for sub in ast.walk(target):
                    if isinstance(sub, ast.Name):
                        names.add(sub.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def test_every_notebook_is_present():
    found = sorted(p.stem for p in NOTEBOOK_DIR.glob("*.ipynb"))
    assert found == sorted(ALL_NOTEBOOKS)


@pytest.mark.parametrize("name", ALL_NOTEBOOKS)
def test_notebook_is_valid_and_targets_pyspark(name):
    nb = json.loads(notebook_path(name).read_text(encoding="utf-8"))

    assert nb["nbformat"] == 4
    assert nb["metadata"]["kernelspec"]["name"] == "synapse_pyspark"
    assert nb["cells"], "notebook has no cells"


@pytest.mark.parametrize("name", ALL_NOTEBOOKS)
def test_every_code_cell_compiles(name):
    """A syntax error must fail here, not on the first scheduled run."""
    for number, source in executable_cells(name, include_entrypoint=True):
        compile(source, cell_filename(name, number), "exec")


@pytest.mark.parametrize("name", ALL_NOTEBOOKS)
def test_a_run_cell_holds_nothing_but_the_run_magic(name):
    """Otherwise the code sharing that cell is invisible to every test here.

    The loader skips a `%run` cell whole — it has to, `%run` is not Python — so
    a statement appended below one would never be compiled or executed by the
    suite, while Fabric would run it happily.
    """
    for cell in notebook_cells(name):
        if cell["cell_type"] != "code":
            continue
        source = "".join(cell["source"])
        if not is_run_magic(source):
            continue
        assert len(source.strip().splitlines()) == 1, (
            f"{name}: a %run cell also contains code, which no test can see:\n{source}"
        )


@pytest.mark.parametrize("name", ALL_NOTEBOOKS)
def test_no_lakehouse_files_paths_survive(name):
    """The whole point of the move: nothing may depend on Lakehouse Files."""
    text = notebook_path(name).read_text(encoding="utf-8")
    assert "/lakehouse/" not in text


@pytest.mark.parametrize("name", ALL_NOTEBOOKS)
def test_run_magics_point_at_notebooks_that_exist(name):
    for target in _run_targets(name):
        assert notebook_path(target).exists(), f"{name}: %run {target} does not exist"


@pytest.mark.parametrize("name", LIBRARY_NOTEBOOKS)
def test_library_notebooks_have_no_run_magics(name):
    """Libraries are leaves: the entry point composes them, in a known order."""
    assert _run_targets(name) == []


@pytest.mark.parametrize("name", LIBRARY_NOTEBOOKS)
def test_library_notebooks_define_their_public_names(name):
    defined = _top_level_names(name)
    missing = [n for n in PUBLIC_NAMES[name] if n not in defined]
    assert not missing, f"{name} no longer defines: {missing}"


@pytest.mark.parametrize("name", LIBRARY_NOTEBOOKS)
def test_library_notebooks_have_no_entrypoint_cell(name):
    """A library that runs something on %run would fire on every import."""
    assert not any(is_entrypoint(c) for c in notebook_cells(name))


@pytest.mark.parametrize("name", ENTRY_NOTEBOOKS)
def test_entry_notebooks_end_with_exactly_one_entrypoint_cell(name):
    cells = [c for c in notebook_cells(name) if c["cell_type"] == "code"]
    assert [is_entrypoint(c) for c in cells].count(True) == 1
    assert is_entrypoint(cells[-1]), "the entrypoint must be the last cell"


@pytest.mark.parametrize("name", ENTRY_NOTEBOOKS)
def test_entry_notebooks_run_the_engine_before_using_it(name):
    targets = _run_targets(name)
    assert "QC_Engine" in targets
    assert "QC_Config" in targets
    assert targets.index("QC_Config") < targets.index("QC_Engine")


@pytest.mark.parametrize("name", ENTRY_NOTEBOOKS)
def test_no_name_is_defined_twice_in_one_flattened_namespace(name):
    """`%run` flattens everything into one namespace — collisions silently win.

    `main` used to be defined by both the runner and preflight; composing them
    in Fabric would have meant one of the two quietly shadowing the other.
    """
    seen: dict[str, str] = {}
    clashes = []
    for source in [*_run_targets(name), name]:
        for defined in sorted(_top_level_names(source)):
            if defined in seen:
                clashes.append(f"{defined}: {seen[defined]} and {source}")
            seen[defined] = source
    assert not clashes, f"{name} composes conflicting definitions:\n  " + "\n  ".join(clashes)
