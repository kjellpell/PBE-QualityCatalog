# Template / reference copy — deploy to /lakehouse/default/Files/Configs/ on Fabric.
# This file is NOT loaded at runtime; the Lakehouse copy is always used.
"""Runtime controls for the Quality Catalog validation runner."""

DRY_RUN = False
FAIL_ON_EMPTY_RULES = True
FAIL_ON_EMPTY_SOURCE = True

MAX_RETRIES = 2
RETRYABLE_ERROR_MARKERS = [
    "timeout",
    "temporar",
    "connection",
    "unavailable",
    "throttle",
]