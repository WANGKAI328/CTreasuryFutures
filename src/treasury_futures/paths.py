"""Canonical project paths shared by scripts and notebooks."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
DATABASE_DIR = DATA_DIR / "database"
DB_PATH = DATABASE_DIR / "treasury_futures.duckdb"

FACTOR_ROOT = PROJECT_ROOT / "factors" / "cicc_close_session_reverse"
FACTOR_NOTEBOOK_DIR = FACTOR_ROOT / "notebooks"
FACTOR_SQL_DIR = FACTOR_ROOT / "sql"
FACTOR_WORKING_DIR = FACTOR_ROOT / "working"
FACTOR_OUTPUT_DIR = FACTOR_ROOT / "output"

DATA_PIPELINE_NOTEBOOK_DIR = PROJECT_ROOT / "notebooks" / "data_pipeline"
DATA_PIPELINE_REPORT_DIR = PROJECT_ROOT / "reports" / "data_pipeline"
