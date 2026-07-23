"""数据管线脚本和 Notebook 共用的项目路径。"""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
DATABASE_DIR = DATA_DIR / "database"
DB_PATH = DATABASE_DIR / "treasury_futures.duckdb"

# CSV-first 数据层：Wind 和手工输入必须先落到这里，DuckDB 只从这些 CSV 同步。
CSV_DATA_DIR = DATA_DIR / "csv"
REFERENCE_CSV_DIR = CSV_DATA_DIR / "reference"
DAILY_CSV_DIR = CSV_DATA_DIR / "daily"
MINUTE_CSV_DIR = CSV_DATA_DIR / "minute"
ECO_CALENDAR_RAW_DIR = CSV_DATA_DIR / "eco_calendar"
HISTORICAL_MINUTE_CSV = CSV_DATA_DIR / "T_mindf.csv"
ECO_CALENDAR_XLSX_PATH = INPUT_DIR / "eco_calendar_filtered.xlsx"
ECO_CALENDAR_CSV_PATH = REFERENCE_CSV_DIR / "eco_calendar_filtered.csv"
CONTRACTS_CSV_PATH = REFERENCE_CSV_DIR / "contracts.csv"
MAIN_MAPPING_CSV_PATH = REFERENCE_CSV_DIR / "main_contract_mapping.csv"

BACKUP_DIR = DATA_DIR / "backups"
DUCKDB_BACKUP_DIR = BACKUP_DIR / "duckdb"
LOG_DIR = PROJECT_ROOT / "logs"
DATA_PIPELINE_LOG_DIR = LOG_DIR / "datapipeline"

DATA_PIPELINE_NOTEBOOK_DIR = PROJECT_ROOT / "notebooks" / "datapipeline"
DATA_PIPELINE_REPORT_DIR = PROJECT_ROOT / "reports" / "data_pipeline"
