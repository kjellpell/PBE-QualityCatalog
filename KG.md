Pre-production checklist
Deploy order (your DEPLOY.md already says this, but it's now stricter — setup.py loads config at startup and will abort if config is missing):

Deploy QualityCatalogConfig.py and QualityCatalogRuntime.py to /lakehouse/default/Files/Configs/
Set REQUIRE_LAKEHOUSE_CONFIG=1 in the environment so the local fallback is disabled
Set DRY_RUN = False in the Lakehouse config (the local fallback now defaults to True — that was one of the bug fixes)
Run setup, then preflight, then a dry run, then flip to production
Verify Fabric working directory — run this in a blank notebook cell before your first real run:


import os, pathlib
print(pathlib.Path.cwd())
print(os.listdir('.'))
You should see the repo root and engine/, rules/, etc. as direct children. If not, RULES_DIR in your Lakehouse config needs to be an absolute path (e.g. /lakehouse/default/Files/rules).

FAIL_ON_EMPTY_SOURCE = True — This is in the local fallback config and appropriate for production. Just be aware: if a source table hasn't been populated yet on the first run, the entire validation will abort with an error. Set it to False in the Lakehouse config during initial data loading if needed.

Run the tests before promoting — from the repo root on any machine with PySpark:


pytest tests/ -v
Both test_expectations.py and test_yaml_rules.py are there and should all pass before you deploy.