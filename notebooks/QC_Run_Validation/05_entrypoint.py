# ENTRYPOINT — the cell that runs this notebook.
configure(QUALITY_CATALOG_CONFIG, QUALITY_CATALOG_RUNTIME)

results_count, violations_count = run_with_metrics(RULE_CATALOG_SOURCES, "run_validation")
print(f"\nResult rows: {results_count}   Violations processed: {violations_count}")

print_run_evidence(QUALITY_CATALOG_CONFIG)
