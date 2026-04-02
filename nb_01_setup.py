# =============================================================================
# NB_01_SETUP.py
# Creates all Delta tables needed for the governance system.
# Run once. Safe to re-run — uses CREATE IF NOT EXISTS.
# No shortcuts prefix on source tables (Fabric Shortcuts limitation).
# =============================================================================

# CELL 1 — Spark session
# -----------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

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


# CELL 3 — gebyr_reference
# Machine-readable version of gebyrforskriften.
# Used by invoice control notebook.
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS gebyr_reference (
    varenr                STRING,
    beskrivelse           STRING,
    forskrift_ref         STRING,
    check_type            STRING,    -- 'EXACT' | 'MIN' | 'COMPONENT' | 'FORBIDDEN'
    expected_amount       DOUBLE,
    min_amount            DOUBLE,
    applies_vedtakstype   STRING,    -- pipe-separated or NULL
    applies_sakstype      STRING,    -- pipe-separated or NULL
    forbidden_vedtakstype STRING,
    valid_from            DATE,
    valid_to              DATE
)
USING DELTA
""")
print("gebyr_reference ready.")


# CELL 4 — invoice_violation
# Written nightly by nb_04_invoice_control.
# Tracks detected invoice violations against gebyrforskriften.
# Read by Power BI finance governance page.
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS invoice_violation (
    violation_id         STRING,     -- SHA2(alert_hash) deterministic surrogate
    alert_hash           STRING,
    violation_type       STRING,     -- 'FAKTURA_AVVIK_EKSAKT' | 'FAKTURA_UNDER_MINIMUM' | 'TAPT_GEBYR'
    prosess_id           BIGINT,
    saksnummer           STRING,
    indikator            STRING,
    enhets_id            BIGINT,
    enhet_navn           STRING,
    saksbehandler_kode   STRING,
    saksbehandler_navn   STRING,
    severity             STRING,
    alert_message        STRING,
    detected_at          TIMESTAMP,
    first_detected_at    TIMESTAMP,
    batch_id             STRING,
    status               STRING,
    resolved_at          TIMESTAMP,
    resolved_method      STRING
)
USING DELTA
""")
print("invoice_violation ready.")


# CELL 5 — Seed gebyr_reference (gebyrforskriften 2026)
# Fixed-fee items where we can verify exactly.
# -----------------------------------------------------------------------------
from datetime import date
today = str(date.today())

spark.sql("DELETE FROM gebyr_reference")

gref = [
    # Grunngebyr §3-2
    ("6321",  "Grunngebyr elektronisk",          "§3-2-1",  "EXACT", 2250.0, None,  None, "Byggesak", None),
    ("6322",  "Grunngebyr papir/e-post",          "§3-2-2",  "EXACT", 3760.0, None,  None, "Byggesak", None),
    # Administrasjonsgebyr — only on andregangsvedtak
    ("6323",  "Administrasjonsgebyr",             "§3-2-3",  "EXACT", 1500.0, None,
     "Igangsettingstillatelse|Brukstillatelse|Ferdigattest", "Byggesak", None),
    ("6323",  "Administrasjonsgebyr forbudt",     "§3-2-3",  "FORBIDDEN", None, None,
     None, None, "Rammetillatelse|Byggesak 12 uker"),
    # Andregangsvedtak §3-9
    ("63911", "IGT nr. 2+",                       "§3-9-1-1","EXACT", 4500.0, None,
     "Igangsettingstillatelse", "Byggesak", None),
    ("63912", "Midlertidig brukstillatelse",      "§3-9-1-2","EXACT", 4500.0, None,
     "Brukstillatelse", "Byggesak", None),
    ("63913", "Ferdigattest etter 5 år",          "§3-9-1-3","EXACT", 7500.0, None,
     "Ferdigattest", "Byggesak", None),
    # Delesak grunngebyr §6-2
    ("6621",  "Grunngebyr fradeling eiendom",     "§6-2-1",  "EXACT", 34290.0, None, None, "Delesak", None),
    ("6623",  "Grunngebyr arealoverføring",       "§6-2-3",  "EXACT", 18180.0, None, None, "Delesak", None),
    ("6624",  "Grunngebyr fradeling anleggseiendom","§6-2-4","EXACT", 67560.0, None, None, "Delesak", None),
    # Tilleggsgebyrer §3-10
    ("63101", "Unntak tekniske krav",             "§3-10-1", "EXACT", 15010.0, None, None, "Byggesak", None),
    ("63102", "Dispensasjon lov/forskrift",       "§3-10-2", "EXACT", 15010.0, None, None, "Byggesak", None),
    ("63103", "Dispensasjon reguleringsplan",     "§3-10-3", "EXACT", 15010.0, None, None, "Byggesak", None),
    ("63104", "Dispensasjon rekkefølgekrav",      "§3-10-4", "EXACT", 18020.0, None, None, "Byggesak", None),
    ("63106", "Dispensasjon 100-metersbeltet",    "§3-10-6", "EXACT", 18020.0, None, None, "Byggesak", None),
    # Minimum floors §3-7 (no BRA available — floor check only)
    ("63711", "Bolig <=4 boenheter (min)",        "§3-7-1",  "MIN",  None, 30040.0,
     "Byggesak 12 uker", "Byggesak", None),
    ("63721", "Boligbygg >4 boenheter (min)",     "§3-7-2",  "MIN",  None, 65700.0,
     "Byggesak 12 uker", "Byggesak", None),
]

gref_schema = StructType([
    StructField("varenr",                StringType(), False),
    StructField("beskrivelse",           StringType(), False),
    StructField("forskrift_ref",         StringType(), False),
    StructField("check_type",            StringType(), False),
    StructField("expected_amount",       DoubleType(), True),
    StructField("min_amount",            DoubleType(), True),
    StructField("applies_vedtakstype",   StringType(), True),
    StructField("applies_sakstype",      StringType(), True),
    StructField("forbidden_vedtakstype", StringType(), True),
])

(spark.createDataFrame(gref, schema=gref_schema)
 .withColumn("valid_from", F.to_date(F.lit(today)))
 .withColumn("valid_to",   F.lit(None).cast(DateType()))
 .write.mode("append").saveAsTable("gebyr_reference"))

print(f"gebyr_reference seeded: {len(gref)} rows.")
print("\n=== SETUP COMPLETE ===")
print("Delta tables: capacity_risk_signal, gebyr_reference, invoice_violation")
