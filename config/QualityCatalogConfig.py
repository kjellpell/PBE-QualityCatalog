# Template / reference copy — deploy to /lakehouse/default/Files/configs/ on Fabric.
# This file is NOT loaded at runtime; the Lakehouse copy is always used.

DEFAULT_SCHEMA = "datakvalitet"


LAKEHOUSE_FILES_DIR = "/lakehouse/default/Files"
RULES_DIR = f"{LAKEHOUSE_FILES_DIR}/rules/gold"
ENGINE_DIR = f"{LAKEHOUSE_FILES_DIR}/engine"
CONFIGS_DIR = f"{LAKEHOUSE_FILES_DIR}/configs"

DQ_RESULTS_TABLE = "dq_run_results"
DQ_VIOLATIONS_TABLE = "dq_violations"
DQ_EXECUTION_METRICS_TABLE = "dq_execution_metrics"