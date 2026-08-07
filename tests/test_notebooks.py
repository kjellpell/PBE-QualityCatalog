"""
Guards on the notebooks themselves.

The notebooks are the deployable artifact, so the things that break a Fabric
run — a cell that will not compile, a `%run` pointing at a notebook that was
renamed, two notebooks defining the same name into one flattened namespace —
have to fail here rather than in the workspace.
"""

import ast
import builtins

import pytest

from tests.notebook_source import (
    NOTEBOOK_DIR,
    load_notebook,
    cell_filename,
    cell_paths,
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
        if is_run_magic("".join(cell["source"]))
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


def _bound_names(name: str) -> set[str]:
    """Everything a notebook binds at top level, imports included."""
    names = _top_level_names(name)
    for node in ast.walk(ast.parse(notebook_code(name, include_entrypoint=True))):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add((alias.asname or alias.name).split(".")[0])
    return names


def _free_names(name: str) -> set[tuple[str, str]]:
    """``(function, name)`` for every global a notebook's functions read.

    Only top-level functions are analysed, and each is walked whole. A nested
    function closes over its parent's locals, so treating it as its own scope
    would report every closed-over name as a missing global.
    """
    free = set()
    tree = ast.parse(notebook_code(name, include_entrypoint=True))
    top_level = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            top_level.append(node)
        elif isinstance(node, ast.ClassDef):
            top_level.extend(
                n for n in node.body
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            )

    for function in top_level:
        bound = set()
        args = function.args
        for arg in [*args.posonlyargs, *args.args, *args.kwonlyargs]:
            bound.add(arg.arg)
        for extra in (args.vararg, args.kwarg):
            if extra:
                bound.add(extra.arg)
        for node in ast.walk(function):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                bound.add(node.name)
            elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                bound.add(node.id)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    bound.add((alias.asname or alias.name).split(".")[0])
            elif isinstance(node, ast.ExceptHandler) and node.name:
                bound.add(node.name)
            elif isinstance(node, ast.arg):
                bound.add(node.arg)
        for node in ast.walk(function):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                if node.id not in bound:
                    free.add((function.name, node.id))
    return free


def test_every_notebook_is_present():
    found = sorted(p.name for p in NOTEBOOK_DIR.iterdir() if p.is_dir())
    assert found == sorted(ALL_NOTEBOOKS)


@pytest.mark.parametrize("name", ALL_NOTEBOOKS)
def test_cell_files_are_numbered_from_01_without_gaps(name):
    """The `NN_` prefix *is* the cell order — nothing else records it.

    Zero-padded and contiguous, so that sorting the filenames gives the order
    the cells have to be pasted in, and a missing number is a missing cell
    rather than a silent renumbering of everything after it.
    """
    paths = cell_paths(name)
    assert paths, f"{name} has no cells"

    prefixes = [p.name.split("_")[0] for p in paths]
    assert prefixes == [f"{i:02d}" for i in range(1, len(paths) + 1)], (
        f"{name}: cell files are not numbered 01..{len(paths):02d}: "
        f"{[p.name for p in paths]}"
    )


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
        source = "".join(cell["source"])
        if not is_run_magic(source):
            continue
        assert len(source.strip().splitlines()) == 1, (
            f"{name}: a %run cell also contains code, which no test can see:\n{source}"
        )


@pytest.mark.parametrize("name", ALL_NOTEBOOKS)
def test_no_lakehouse_files_paths_survive(name):
    """The whole point of the move: nothing may depend on Lakehouse Files."""
    for path in cell_paths(name):
        assert "/lakehouse/" not in path.read_text(encoding="utf-8"), path


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
def test_the_entrypoint_marker_is_visible_to_whoever_pastes_it(name):
    """The marker is a comment precisely so it survives the paste."""
    entry = [c for c in notebook_cells(name) if is_entrypoint(c)][0]
    assert "".join(entry["source"]).startswith("# ENTRYPOINT")


@pytest.mark.parametrize("name", ENTRY_NOTEBOOKS)
def test_entry_notebooks_end_with_exactly_one_entrypoint_cell(name):
    cells = notebook_cells(name)
    assert [is_entrypoint(c) for c in cells].count(True) == 1
    assert is_entrypoint(cells[-1]), "the entrypoint must be the last cell"


@pytest.mark.parametrize("name", ALL_NOTEBOOKS)
def test_a_notebook_is_one_body_cell(name):
    """Deployment is copy-paste, so every extra cell is another manual step.

    `%run` has to stand alone and the entry point has to stay separately
    runnable; everything else belongs in one cell. Splitting the body again
    would quietly turn a seventeen-paste deployment into a fifty-paste one.
    """
    body = [
        c for c in notebook_cells(name)
        if not is_run_magic("".join(c["source"])) and not is_entrypoint(c)
    ]
    assert len(body) == 1, f"{name} has {len(body)} body cells, expected 1"


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


@pytest.mark.parametrize("name", ALL_NOTEBOOKS)
def test_every_free_name_resolves_in_the_composed_namespace(name):
    """A NameError here would only ever show up on a Fabric run.

    `%run` composition means a function can legally reach for a name another
    notebook defines — `print_run_evidence` calls `_qualify`, which lives in
    QC_Engine. Nothing executes those functions in this suite, so without this
    check a rename in one notebook would break another silently, and the first
    sign of it would be a failed nightly run.
    """
    available = set(dir(builtins))
    for target in _run_targets(name):
        available |= _bound_names(target)
    available |= _bound_names(name)

    unresolved = sorted(
        f"{function}() reads '{free}'"
        for function, free in _free_names(name)
        if free not in available
    )
    assert not unresolved, f"{name} would NameError in Fabric:\n  " + "\n  ".join(unresolved)


def test_the_shipped_config_satisfies_the_engine():
    """QC_Config's *values* are otherwise never checked.

    The other guards here are static, and the suite runs the engine against its
    own test config — so a key deleted from QUALITY_CATALOG_CONFIG would ship,
    and fail at configure() on the first run in the workspace.
    """
    engine = load_notebook("QC_Engine")
    config = load_notebook("QC_Config")

    engine.build_settings(
        config.QUALITY_CATALOG_CONFIG, engine.CONFIG_REQUIRED_KEYS, "QUALITY_CATALOG_CONFIG"
    )
    engine.build_settings(
        config.QUALITY_CATALOG_RUNTIME, engine.RUNTIME_REQUIRED_KEYS, "QUALITY_CATALOG_RUNTIME"
    )

    # Same call configure() makes; rejects a table name that would not survive
    # being interpolated into SQL.
    targets = engine.resolve_targets(
        engine.build_settings(config.QUALITY_CATALOG_CONFIG, [], "QUALITY_CATALOG_CONFIG")
    )
    assert set(targets) == {"results_table", "violations_table", "execution_metrics_table"}


@pytest.mark.parametrize("name", ALL_NOTEBOOKS)
def test_a_cell_file_is_nothing_but_the_cell(name):
    """A file goes into a cell whole, so it may hold nothing structural.

    Fabric's own export writes a `# Fabric notebook source` header, a
    `# CELL ****` line between cells and a `# META` block after each one. All
    three are instructions to the reader about where a cell begins and ends —
    and this project has twice shipped a notebook where they were pasted *into*
    a cell instead, killing the `%run` with MagicUsageError. There is nothing
    left to interpret now: the file boundary is the cell boundary.

    Checked against the file text, because the file is what gets pasted.
    """
    for path in cell_paths(name):
        offenders = [
            line for line in path.read_text(encoding="utf-8").splitlines()
            if line.startswith(("# META", "# CELL ", "# Fabric notebook source"))
        ]
        assert not offenders, (
            f"{path.relative_to(NOTEBOOK_DIR.parent)} carries notebook structure "
            f"that would be pasted into the cell:\n  " + "\n  ".join(offenders[:5])
        )
