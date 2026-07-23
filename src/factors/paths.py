"""因子研究脚本和 Notebook 共用的项目路径。"""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
DATABASE_DIR = DATA_DIR / "database"
DB_PATH = DATABASE_DIR / "treasury_futures.duckdb"

FACTOR_ROOT = PROJECT_ROOT / "factors" / "cicc_close_session_reverse"
FACTOR_NOTEBOOK_DIR = PROJECT_ROOT / "notebooks" / "factors"
FACTOR_SQL_DIR = FACTOR_ROOT / "sql"
FACTOR_WORKING_DIR = FACTOR_ROOT / "working"
FACTOR_OUTPUT_DIR = FACTOR_ROOT / "output"

LOG_DIR = PROJECT_ROOT / "logs"
FACTOR_LOG_DIR = LOG_DIR / "factors"
