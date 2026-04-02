# =============================================================================
# NB_01_SETUP.py
# Creates all Delta tables needed for the governance system.
# Run once. Safe to re-run — uses CREATE IF NOT EXISTS.
# No shortcuts prefix on source tables (Fabric Shortcuts limitation).
# =============================================================================

# CELL 1 — Spark session
# -----------------------------------------------------------------------------
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")
print("Spark ready.")


# CELL 2 — capacity_risk_signal
# Written nightly by nb_03_capacity.
# One row per unit per week. Tracks production rate trend.
# Used by Power BI business team page for capacity ceiling detection.
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS capacity_risk_signal (
    signal_id            STRING,     -- SHA2(enhets_id | uke_dato | batch_id) deterministic surrogate
    enhets_id            BIGINT,
    enhet_navn           STRING,
    seksjon_navn         STRING,
    avdeling_navn        STRING,
    uke_dato             DATE,        -- Monday of the week
    mottatt              INT,         -- received this week
    produsert            INT,         -- produced this week
    portefolje           INT,         -- open portfolio at week end
    produksjonsrate      DOUBLE,      -- produsert / mottatt
    portefolje_trend_3u  DOUBLE,      -- portfolio change over 3 weeks
    snitt_behandlingstid DOUBLE,      -- avg days to seneste_stoppmilepael
    snitt_tidlig_dager   DOUBLE,      -- avg days to Tidligbehandling_dato
    pct_innenfor_frist   DOUBLE,      -- % within deadline this week
    risk_level           STRING,      -- 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
    risk_reason          STRING,      -- plain Norwegian explanation
    computed_at          TIMESTAMP,
    batch_id             STRING
)
USING DELTA
""")
print("capacity_risk_signal ready.")


print("\n=== SETUP COMPLETE ===")
print("Delta tables: capacity_risk_signal")
