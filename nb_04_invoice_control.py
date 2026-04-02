# =============================================================================
# NB_04_INVOICE_CONTROL.py
# Checks invoices against gebyrforskriften.
# Covers Fakturalinjer (2026+) and Fakturering+Bilag (pre-2026).
# Schedule: nightly.
# =============================================================================

# CELL 1 — Imports and batch ID
# -----------------------------------------------------------------------------
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")

BATCH_ID           = datetime.now().strftime("%Y-%m-%d-invoice")
ROUNDING_TOLERANCE = 1.0

print(f"Batch: {BATCH_ID}")


# CELL 1b — Load source tables as temp views
# -----------------------------------------------------------------------------
spark.read.table("Saksbehandling.Prosesser")\
     .createOrReplaceTempView("Prosesser")
spark.read.table("Saksbehandling.Fakturalinjer")\
     .createOrReplaceTempView("Fakturalinjer")
spark.read.table("Okonomi.Fakturering")\
     .createOrReplaceTempView("Fakturering")
spark.read.table("Okonomi.Bilag")\
     .createOrReplaceTempView("Bilag")
spark.read.table("Felles.Organisasjon")\
     .createOrReplaceTempView("Organisasjon")

print("Source tables loaded as temp views.")


# CELL 2 — Build unified invoice lines
# Combines Fakturalinjer (2026+) with Fakturering+Bilag (pre-2026).
# No pricelist join needed here — varenr descriptions come from gebyr_reference
# which is already joined in each violation check cell.
# -----------------------------------------------------------------------------
fl_df = spark.sql("""
    SELECT
        fl.Fakturanr, fl.Prosess_id, fl.Fakturert_dato, fl.Betalt_dato,
        fl.Status AS faktura_status,
        fl.Linje_belop AS linje_belop,
        fl.Fakturasum AS fakturasum,
        fl.Tapt_gebyr,
        CAST(fl.Produktnr AS STRING) AS varenr,
        fl.Linje_beskrivelse AS linje_beskrivelse,
        p.Indikator, p.Saksnummer, p.Enhets_id,
        p.Enhetskode, p.Saksbehandler_kode, p.Saksbehandler_navn,
        COALESCE(o.Enhet_navn, p.Enhetskode,
            CAST(p.Enhets_id AS STRING)) AS Enhet_navn,
        o.Seksjon_navn,
        '2026+' AS kilde
    FROM Fakturalinjer fl
    JOIN Prosesser p ON fl.Prosess_id = p.Prosess_id
    LEFT JOIN Organisasjon o ON p.Enhets_id = o.Enhets_id
    WHERE fl.Fakturert_dato IS NOT NULL
""")

f_df = spark.sql("""
    SELECT
        f.Fakturanr, b.Prosess_id,
        b.Bilagsdato AS Fakturert_dato,
        NULL AS Betalt_dato,
        f.Status AS faktura_status,
        f.Belop AS linje_belop,
        f.Belop AS fakturasum,
        NULL AS Tapt_gebyr,
        CAST(f.Artikkel AS STRING) AS varenr,
        f.Fakturalinje AS linje_beskrivelse,
        p.Indikator, p.Saksnummer, p.Enhets_id,
        p.Enhetskode, p.Saksbehandler_kode, p.Saksbehandler_navn,
        COALESCE(o.Enhet_navn, p.Enhetskode,
            CAST(p.Enhets_id AS STRING)) AS Enhet_navn,
        o.Seksjon_navn,
        'pre-2026' AS kilde
    FROM Fakturering f
    JOIN Bilag b ON f.Fakturanr = b.Bilagsnr
    JOIN Prosesser p ON b.Prosess_id = p.Prosess_id
    LEFT JOIN Organisasjon o ON p.Enhets_id = o.Enhets_id
    WHERE b.Bilagsdato IS NOT NULL
""")

invoice_all = fl_df.union(f_df)
invoice_all.createOrReplaceTempView("invoice_lines_unified")

total_lines = invoice_all.count()
print(f"Invoice lines loaded: {total_lines:,}")


# CELL 3 — Load current gebyr_reference
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW ref AS
    SELECT *
    FROM gebyr_reference
    WHERE valid_from <= CURRENT_DATE()
    AND  (valid_to IS NULL OR valid_to >= CURRENT_DATE())
""")
print("Gebyr reference loaded.")


# CELL 4 — EXACT checks
# -----------------------------------------------------------------------------
exact_violations = spark.sql(f"""
    SELECT
        il.Fakturanr, il.Prosess_id, il.Saksnummer, il.Indikator,
        il.Enhets_id, il.Enhet_navn,
        il.Saksbehandler_kode, il.Saksbehandler_navn,
        il.varenr, r.beskrivelse AS varenr_beskrivelse,
        il.linje_belop AS fakturert_belop,
        r.expected_amount, r.forskrift_ref,
        'FAKTURA_AVVIK_EKSAKT'  AS violation_type,
        'Error'                 AS severity,
        SHA2(CONCAT_WS('|',
            'INV_EXACT',
            CAST(il.Fakturanr AS STRING),
            CAST(il.Prosess_id AS STRING),
            il.varenr,
            CAST(ROUND(il.linje_belop, 2) AS STRING),
            CAST(ROUND(r.expected_amount, 2) AS STRING)
        ), 256) AS alert_hash,
        CONCAT(
            'Fakturalinje ', il.varenr,
            CASE WHEN r.beskrivelse IS NOT NULL
                 THEN CONCAT(' (', r.beskrivelse, ')') ELSE '' END,
            ': fakturert kr ', CAST(CAST(il.linje_belop AS INT) AS STRING),
            ', forventet kr ', CAST(CAST(r.expected_amount AS INT) AS STRING),
            ' iht. ', r.forskrift_ref, '.'
        ) AS alert_message
    FROM invoice_lines_unified il
    JOIN ref r
        ON  il.varenr = r.varenr
        AND r.check_type = 'EXACT'
        AND (r.applies_sakstype IS NULL
             OR FIND_IN_SET(il.Indikator,
                REPLACE(r.applies_sakstype, '|', ',')) > 0)
    WHERE ABS(il.linje_belop - r.expected_amount) > {ROUNDING_TOLERANCE}
""")
exact_violations.createOrReplaceTempView("v_inv_exact")
print(f"EXACT violations:       {exact_violations.count():>5}")


# CELL 5 — MINIMUM FLOOR checks
# -----------------------------------------------------------------------------
min_violations = spark.sql(f"""
    SELECT
        il.Fakturanr, il.Prosess_id, il.Saksnummer, il.Indikator,
        il.Enhets_id, il.Enhet_navn,
        il.Saksbehandler_kode, il.Saksbehandler_navn,
        il.varenr, r.beskrivelse AS varenr_beskrivelse,
        il.linje_belop AS fakturert_belop,
        r.min_amount, r.forskrift_ref,
        'FAKTURA_UNDER_MINIMUM' AS violation_type,
        'Warning'               AS severity,
        SHA2(CONCAT_WS('|',
            'INV_MIN',
            CAST(il.Fakturanr AS STRING),
            CAST(il.Prosess_id AS STRING),
            il.varenr,
            CAST(ROUND(il.linje_belop, 2) AS STRING),
            CAST(ROUND(r.min_amount, 2) AS STRING)
        ), 256) AS alert_hash,
        CONCAT(
            'Fakturalinje ', il.varenr,
            CASE WHEN r.beskrivelse IS NOT NULL
                 THEN CONCAT(' (', r.beskrivelse, ')') ELSE '' END,
            ': fakturert kr ', CAST(CAST(il.linje_belop AS INT) AS STRING),
            ' er under laveste mulige gebyr kr ',
            CAST(CAST(r.min_amount AS INT) AS STRING),
            ' iht. ', r.forskrift_ref, '.'
        ) AS alert_message
    FROM invoice_lines_unified il
    JOIN ref r
        ON  il.varenr = r.varenr
        AND r.check_type = 'MIN'
        AND (r.applies_sakstype IS NULL
             OR FIND_IN_SET(il.Indikator,
                REPLACE(r.applies_sakstype, '|', ',')) > 0)
    WHERE il.linje_belop < r.min_amount
""")
min_violations.createOrReplaceTempView("v_inv_min")
print(f"BELOW_MINIMUM:          {min_violations.count():>5}")


# CELL 6 — TAPT_GEBYR check (2026+ only)
# -----------------------------------------------------------------------------
tapt_violations = spark.sql(f"""
    SELECT
        il.Fakturanr, il.Prosess_id, il.Saksnummer, il.Indikator,
        il.Enhets_id, il.Enhet_navn,
        il.Saksbehandler_kode, il.Saksbehandler_navn,
        il.varenr, gr.beskrivelse AS varenr_beskrivelse,
        il.linje_belop AS fakturert_belop,
        il.Tapt_gebyr,
        NULL AS forskrift_ref,
        'TAPT_GEBYR'   AS violation_type,
        'Warning'      AS severity,
        SHA2(CONCAT_WS('|',
            'INV_TAPT',
            CAST(il.Fakturanr AS STRING),
            CAST(il.Prosess_id AS STRING),
            il.varenr,
            CAST(ROUND(il.Tapt_gebyr, 2) AS STRING)
        ), 256) AS alert_hash,
        CONCAT(
            'Tapt gebyr på kr ',
            CAST(CAST(il.Tapt_gebyr AS INT) AS STRING),
            ' registrert på prosess ', CAST(il.Prosess_id AS STRING),
            '. Årsak: fristoverskridelse.'
        ) AS alert_message
    FROM invoice_lines_unified il
    LEFT JOIN (
        SELECT varenr, MIN(beskrivelse) AS beskrivelse
        FROM ref
        GROUP BY varenr
    ) gr ON il.varenr = gr.varenr
    WHERE il.kilde = '2026+'
    AND   il.Tapt_gebyr IS NOT NULL
    AND   il.Tapt_gebyr > 0
""")
tapt_violations.createOrReplaceTempView("v_inv_tapt")
print(f"TAPT_GEBYR:             {tapt_violations.count():>5}")


# CELL 7 — Union all invoice violations
# -----------------------------------------------------------------------------
all_invoice_violations = spark.sql("""
    SELECT Fakturanr, Prosess_id, Saksnummer, Indikator,
           Enhets_id, Enhet_navn, Saksbehandler_kode, Saksbehandler_navn,
           varenr, violation_type, severity, alert_hash, alert_message
    FROM v_inv_exact
    UNION ALL
    SELECT Fakturanr, Prosess_id, Saksnummer, Indikator,
           Enhets_id, Enhet_navn, Saksbehandler_kode, Saksbehandler_navn,
           varenr, violation_type, severity, alert_hash, alert_message
    FROM v_inv_min
    UNION ALL
    SELECT Fakturanr, Prosess_id, Saksnummer, Indikator,
           Enhets_id, Enhet_navn, Saksbehandler_kode, Saksbehandler_navn,
           varenr, violation_type, severity, alert_hash, alert_message
    FROM v_inv_tapt
""").cache()
all_invoice_violations.createOrReplaceTempView("all_invoice_violations")
print(f"\nTotal invoice violations: {all_invoice_violations.count():,}")


# CELL 8 — Upsert invoice violations; preserve first_detected_at on re-open
# MERGE is idempotent: re-running never creates duplicates.
# WHEN MATCHED AND OPEN: refresh message + batch only.
# WHEN NOT MATCHED: insert fresh violation.
# -----------------------------------------------------------------------------
spark.sql(f"""
    MERGE INTO invoice_violation d
    USING (
        SELECT
            SHA2(CONCAT_WS('|', v.alert_hash, v.violation_type), 256) AS violation_id,
            v.alert_hash,
            v.violation_type,
            v.Prosess_id, v.Saksnummer, v.Indikator,
            v.Enhets_id, v.Enhet_navn,
            v.Saksbehandler_kode, v.Saksbehandler_navn,
            v.severity, v.alert_message
        FROM all_invoice_violations v
    ) s
    ON  d.alert_hash = s.alert_hash
    AND d.status     = 'OPEN'
    WHEN MATCHED THEN UPDATE SET
        d.alert_message = s.alert_message,
        d.batch_id      = '{BATCH_ID}'
    WHEN NOT MATCHED THEN INSERT (
        violation_id, alert_hash, violation_type,
        prosess_id, saksnummer, indikator, enhets_id, enhet_navn,
        saksbehandler_kode, saksbehandler_navn,
        severity, alert_message,
        detected_at, first_detected_at, batch_id, status
    ) VALUES (
        s.violation_id, s.alert_hash, s.violation_type,
        s.Prosess_id, s.Saksnummer, s.Indikator,
        s.Enhets_id, s.Enhet_navn,
        s.Saksbehandler_kode, s.Saksbehandler_navn,
        s.severity, s.alert_message,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
        '{BATCH_ID}', 'OPEN'
    )
""")
print("Invoice violations upserted.")


# CELL 9 — Auto-resolve invoice violations that no longer fire
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW resolve_invoice_hashes AS
    SELECT DISTINCT ex.alert_hash
    FROM invoice_violation ex
    LEFT ANTI JOIN (
        SELECT DISTINCT alert_hash FROM all_invoice_violations
    ) cur
    ON ex.alert_hash = cur.alert_hash
    WHERE ex.status = 'OPEN'
""")

spark.sql("""
    MERGE INTO invoice_violation d
    USING resolve_invoice_hashes r
    ON  d.alert_hash = r.alert_hash
    AND d.status     = 'OPEN'
    WHEN MATCHED THEN UPDATE SET
        d.status          = 'RESOLVED',
        d.resolved_at     = CURRENT_TIMESTAMP,
        d.resolved_method = 'AUTO_CLEARED'
""")

print("Auto-resolve sweep complete.")


# CELL 10 — Summary
# -----------------------------------------------------------------------------
print("\n=== INVOICE SUMMARY ===")
spark.sql("""
    SELECT
        violation_type AS type, severity, status,
        COUNT(*) AS n,
        COUNT(DISTINCT saksnummer) AS n_saker,
        COUNT(DISTINCT enhet_navn) AS n_enheter
    FROM invoice_violation
    GROUP BY violation_type, severity, status
    ORDER BY violation_type, status
""").show(truncate=False)

print("\n=== OPEN INVOICE VIOLATIONS BY UNIT ===")
spark.sql("""
    SELECT
        enhet_navn,
        COUNT(*)                                               AS open_violations,
        SUM(CASE WHEN severity = 'Error'   THEN 1 ELSE 0 END) AS errors,
        SUM(CASE WHEN severity = 'Warning' THEN 1 ELSE 0 END) AS warnings
    FROM invoice_violation
    WHERE status = 'OPEN'
    GROUP BY enhet_navn
    ORDER BY errors DESC, open_violations DESC
""").show(truncate=False)
