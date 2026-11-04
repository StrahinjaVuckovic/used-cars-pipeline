"""
Helpers for running Databricks notebooks locally in tests.

Databricks notebooks rely on globals that only exist on a cluster (`spark`, `dbutils`, `display`) and load
shared code with `%run` (a comment line in the exported .py file). These helpers execute the notebook
source in a namespace where those globals are replaced by a local SparkSession and simple mocks, so the
production code runs unchanged.
"""

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
DATA_DIR = PROJECT_ROOT / "data"

CELL_SEPARATOR = "# COMMAND ----------"

FAKE_SECRETS = {
    "client-id": "fake-client-id",
    "client-secret": "fake-client-secret",
    "tenant-id": "fake-tenant-id",
}
EXPECTED_SECRET_SCOPE = "key-vault-secrets"


@dataclass
class FileInfo:
    """Mimics the objects returned by dbutils.fs.ls (directory names end with '/')."""

    path: str
    name: str
    size: int

    def isDir(self) -> bool:  # noqa: N802 – same name as the Databricks API
        return self.name.endswith("/")


class _Widgets:
    def __init__(self, values: dict):
        self.values = dict(values)

    def text(self, name, default_value, label=None):
        self.values.setdefault(name, default_value)

    def dropdown(self, name, default_value, choices, label=None):
        self.values.setdefault(name, default_value)

    def get(self, name):
        return self.values[name]


class _Secrets:
    def __init__(self):
        self.requests = []

    def get(self, scope, key):
        self.requests.append((scope, key))
        if scope != EXPECTED_SECRET_SCOPE:
            raise ValueError(f"Unknown secret scope: {scope}")
        return FAKE_SECRETS[key]


class _Fs:
    def ls(self, path):
        local_path = path[len("file:"):] if path.startswith("file:") else path
        items = []
        for entry in sorted(os.scandir(local_path), key=lambda e: e.name):
            name = entry.name + "/" if entry.is_dir() else entry.name
            size = 0 if entry.is_dir() else entry.stat().st_size
            items.append(FileInfo(path=os.path.join(local_path, name), name=name, size=size))
        return items


class DBUtilsMock:
    """Minimal stand-in for dbutils: widgets, secrets and fs.ls on the local file system."""

    def __init__(self, widget_values: dict = None):
        self.widgets = _Widgets(widget_values or {})
        self.secrets = _Secrets()
        self.fs = _Fs()


class DisplayMock:
    """Replaces display(): forces the DataFrame to be computed (so errors surface) and keeps the result."""

    def __init__(self):
        self.results = []

    def __call__(self, df):
        self.results.append(df.collect())


def read_notebook(relative_path: str) -> str:
    return (NOTEBOOKS_DIR / relative_path).read_text(encoding="utf-8")


def run_notebook(relative_path: str, namespace: dict) -> dict:
    """Execute a whole notebook in `namespace` (the `%run` lines are comments and are skipped by Python)."""
    source = read_notebook(relative_path)
    exec(compile(source, str(NOTEBOOKS_DIR / relative_path), "exec"), namespace)
    return namespace


def run_notebook_cells_until(relative_path: str, namespace: dict, stop_marker: str) -> dict:
    """Execute notebook cells in order, stopping before the first cell that contains `stop_marker`."""
    source = read_notebook(relative_path)
    cells = source.split(CELL_SEPARATOR)
    selected = []
    for cell in cells:
        if stop_marker in cell:
            break
        selected.append(cell)
    else:
        raise ValueError(f"Marker {stop_marker!r} not found in {relative_path}")
    exec(compile(CELL_SEPARATOR.join(selected), str(NOTEBOOKS_DIR / relative_path), "exec"), namespace)
    return namespace


def new_namespace(spark, widget_values: dict = None) -> dict:
    """Namespace with the Databricks globals replaced by local objects."""
    return {
        "__name__": "__notebook__",
        "spark": spark,
        "dbutils": DBUtilsMock(widget_values),
        "display": DisplayMock(),
    }


def load_config_and_utils(spark, bronze_path: str, silver_path: str, widget_values: dict = None) -> dict:
    """
    Run 00_config (with mocked secrets) and utils/silver_transformations, then point the storage paths
    to local folders instead of ADLS.
    """
    namespace = new_namespace(spark, widget_values)
    run_notebook("00_config.py", namespace)
    run_notebook("utils/silver_transformations.py", namespace)
    namespace["BRONZE_PATH"] = bronze_path
    namespace["SILVER_PATH"] = silver_path
    return namespace
