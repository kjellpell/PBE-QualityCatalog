# =============================================================================
# NB_02_DQ_DETECTION.py
# Evaluates data quality rules against Prosesser.
# Writes new alerts to dq_alert. Auto-resolves alerts no longer firing.
# Schedule: nightly.
# =============================================================================

# CELL 1 — Imports and batch ID
# -----------------------------------------------------------------------------
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")

BATCH_ID = datetime.now().strftime("%Y-%m-%d-dq")
print(f"Batch: {BATCH_ID}")


# CELL 1b — Load source tables as temp views
# spark.read.table() resolves Shortcuts correctly.
# All subsequent SQL uses these simple temp view names — no schema prefix.
# -----------------------------------------------------------------------------
spark.read.table("Saksbehandling.Prosesser")\
     .createOrReplaceTempView("Prosesser")
spark.read.table("Saksbehandling.Saker")\
     .createOrReplaceTempView("Saker")
spark.read.table("Felles.Organisasjon")\
     .createOrReplaceTempView("Organisasjon")
spark.read.table("Felles.Indikatorer")\
     .createOrReplaceTempView("Indikatorer")

print("Source tables loaded as temp views.")


# CELL 2 — Load active cases with org context
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW active_prosesser AS
    SELECT
        p.Prosess_id,
        p.Saksnummer,
        p.Indikator,
        p.Indikator_aggregert,
        p.Delprosess_navn,
        p.tidligste_startmilepael_dato,
        p.seneste_stoppmilepael_dato,
        p.Tidligbehandling_dato,
        p.Er_mangelfull,
        p.Tidsbruk,
        p.innenfor_frist,
        p.aggregert_frist,
        p.Saksbehandler_kode,
        p.Saksbehandler_navn,
        p.Enhets_id,
        p.Avdelingskode,
        p.Enhetskode,
        COALESCE(o.Enhet_navn, p.Enhetskode, CAST(p.Enhets_id AS STRING)) AS Enhet_navn,
        o.Seksjon_navn,
        o.Avdeling_navn,
        s.Status                                        AS sak_status,
        DATEDIFF(CURRENT_DATE(),
            p.tidligste_startmilepael_dato)             AS dager_siden_mottatt,
        DATEDIFF(p.Tidligbehandling_dato,
            p.tidligste_startmilepael_dato)             AS dager_til_tidlig
    FROM Prosesser p
    LEFT JOIN Saker s
        ON  p.Saksnummer = s.Saksnummer
    LEFT JOIN Organisasjon o
        ON  p.Enhets_id = o.Enhets_id
    WHERE p.tidligste_startmilepael_dato IS NOT NULL
    AND   p.Delprosess_navn NOT IN (
        'Planinitiativ (oppstartsfase før RIPP)',
        'Planskisse (dialogfase før RIPP)'
    )
""")

total = spark.sql("SELECT COUNT(*) FROM active_prosesser").collect()[0][0]
print(f"Active prosesser loaded: {total:,}")


# CELL 3 — Load max_open_days params for Rule 3
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW max_open_params AS
    SELECT
        i.Indikator,
        COALESCE(
            CAST(rp_ind.param_value AS INT),
            CAST(rp_glob.param_value AS INT),
            365
        ) AS max_open_days
    FROM Indikatorer i
    LEFT JOIN dq_rule_param rp_ind
        ON  rp_ind.rule_id    = 3
        AND rp_ind.scope_type = 'INDIKATOR'
        AND rp_ind.scope_key  = i.Indikator
        AND rp_ind.param_name = 'max_open_days'
    LEFT JOIN dq_rule_param rp_glob
        ON  rp_glob.rule_id    = 3
        AND rp_glob.scope_type = 'GLOBAL'
        AND rp_glob.param_name = 'max_open_days'
""")


# CELL 4 — RULE 1: TIDLIG_MANGLER
# Active process received > 28 days ago with no Tidligbehandling_dato.
# -----------------------------------------------------------------------------
r1 = spark.sql(f"""
    SELECT
        r.rule_id, r.rule_code, r.rule_version, r.category, r.severity,
        p.Prosess_id, p.Saksnummer, p.Indikator, p.Enhets_id, p.Enhet_navn,
        p.Saksbehandler_kode, p.Saksbehandler_navn,
        SHA2(CONCAT('TIDLIG_MANGLER|', p.Prosess_id), 256) AS alert_hash,
        CONCAT(
            'Saken ble mottatt ', CAST(p.dager_siden_mottatt AS STRING),
            ' dager siden. Tidligbehandling er ikke registrert. Fristen er 28 dager.'
        ) AS alert_message
    FROM active_prosesser p
    JOIN dq_rule r ON r.rule_code = 'TIDLIG_MANGLER' AND r.active_flag = true
    WHERE p.seneste_stoppmilepael_dato IS NULL
    AND   p.Tidligbehandling_dato IS NULL
    AND   p.dager_siden_mottatt > 28
    AND   p.sak_status <> 'Saken er avsluttet'
""")
r1.createOrReplaceTempView("v_r1")
print(f"TIDLIG_MANGLER:          {r1.count():>5} violations")


# CELL 5 — RULE 2: TIDLIG_FRIST_BRUTT
# Tidligbehandling registered but took more than 28 days.
# Only flag open processes or those closed within last 365 days
# to avoid surfacing thousands of old historical breaches.
# -----------------------------------------------------------------------------
r2 = spark.sql(f"""
    SELECT
        r.rule_id, r.rule_code, r.rule_version, r.category, r.severity,
        p.Prosess_id, p.Saksnummer, p.Indikator, p.Enhets_id, p.Enhet_navn,
        p.Saksbehandler_kode, p.Saksbehandler_navn,
        SHA2(CONCAT('TIDLIG_FRIST_BRUTT|', p.Prosess_id), 256) AS alert_hash,
        CONCAT(
            'Tidligbehandling ble utført ',
            CAST(p.dager_til_tidlig AS STRING),
            ' dager etter mottak. Fristen er 28 dager.'
        ) AS alert_message
    FROM active_prosesser p
    JOIN dq_rule r ON r.rule_code = 'TIDLIG_FRIST_BRUTT' AND r.active_flag = true
    WHERE p.Tidligbehandling_dato IS NOT NULL
    AND   p.tidligste_startmilepael_dato IS NOT NULL
    AND   p.dager_til_tidlig > 28
    AND   (
        p.seneste_stoppmilepael_dato IS NULL
        OR DATEDIFF(CURRENT_DATE(), p.seneste_stoppmilepael_dato) <= 365
    )
""")
r2.createOrReplaceTempView("v_r2")
print(f"TIDLIG_FRIST_BRUTT:      {r2.count():>5} violations")


# CELL 6 — RULE 3: LANG_LIGGETID
# -----------------------------------------------------------------------------
r3 = spark.sql(f"""
    SELECT
        r.rule_id, r.rule_code, r.rule_version, r.category, r.severity,
        p.Prosess_id, p.Saksnummer, p.Indikator, p.Enhets_id, p.Enhet_navn,
        p.Saksbehandler_kode, p.Saksbehandler_navn,
        SHA2(CONCAT('LANG_LIGGETID|', p.Prosess_id), 256) AS alert_hash,
        CONCAT(
            'Prosessen har vært åpen i ',
            CAST(p.dager_siden_mottatt AS STRING),
            ' dager uten ferdigbehandling. Terskel for ',
            p.Indikator, ': ', CAST(mp.max_open_days AS STRING), ' dager.'
        ) AS alert_message
    FROM active_prosesser p
    JOIN max_open_params mp ON p.Indikator = mp.Indikator
    JOIN dq_rule r ON r.rule_code = 'LANG_LIGGETID' AND r.active_flag = true
    WHERE p.seneste_stoppmilepael_dato IS NULL
    AND   p.sak_status <> 'Saken er avsluttet'
    AND   p.dager_siden_mottatt > mp.max_open_days
""")
r3.createOrReplaceTempView("v_r3")
print(f"LANG_LIGGETID:           {r3.count():>5} violations")


# CELL 7 — RULE 4: MANGELFULL_LANG_VENTETID
# -----------------------------------------------------------------------------
r4 = spark.sql(f"""
    SELECT
        r.rule_id, r.rule_code, r.rule_version, r.category, r.severity,
        p.Prosess_id, p.Saksnummer, p.Indikator, p.Enhets_id, p.Enhet_navn,
        p.Saksbehandler_kode, p.Saksbehandler_navn,
        SHA2(CONCAT('MANGELFULL_LANG|', p.Prosess_id), 256) AS alert_hash,
        CONCAT(
            'Prosessen er mangelfull og har ventet ',
            CAST(p.dager_siden_mottatt AS STRING),
            ' dager. Vurder aktiv oppfølging eller avslutning.'
        ) AS alert_message
    FROM active_prosesser p
    JOIN dq_rule r ON r.rule_code = 'MANGELFULL_LANG_VENTETID' AND r.active_flag = true
    WHERE p.Er_mangelfull = 1
    AND   p.seneste_stoppmilepael_dato IS NULL
    AND   p.sak_status <> 'Saken er avsluttet'
    AND   p.dager_siden_mottatt > 90
""")
r4.createOrReplaceTempView("v_r4")
print(f"MANGELFULL_LANG_VENTETID:{r4.count():>5} violations")


# CELL 8 — Combine all case violations
# -----------------------------------------------------------------------------
all_violations = spark.sql("""
    SELECT rule_id, rule_code, rule_version, category, severity,
           Prosess_id, Saksnummer, Indikator, Enhets_id, Enhet_navn,
           Saksbehandler_kode, Saksbehandler_navn, alert_hash, alert_message
    FROM v_r1
    UNION ALL
    SELECT rule_id, rule_code, rule_version, category, severity,
           Prosess_id, Saksnummer, Indikator, Enhets_id, Enhet_navn,
           Saksbehandler_kode, Saksbehandler_navn, alert_hash, alert_message
    FROM v_r2
    UNION ALL
    SELECT rule_id, rule_code, rule_version, category, severity,
           Prosess_id, Saksnummer, Indikator, Enhets_id, Enhet_navn,
           Saksbehandler_kode, Saksbehandler_navn, alert_hash, alert_message
    FROM v_r3
    UNION ALL
    SELECT rule_id, rule_code, rule_version, category, severity,
           Prosess_id, Saksnummer, Indikator, Enhets_id, Enhet_navn,
           Saksbehandler_kode, Saksbehandler_navn, alert_hash, alert_message
    FROM v_r4
""").cache()
all_violations.createOrReplaceTempView("all_case_violations")
print(f"\nTotal case violations: {all_violations.count():,}")


# CELL 9 — Upsert new alerts; preserve first_detected_at on re-open
# MERGE is idempotent: re-running the notebook never creates duplicates.
# WHEN MATCHED AND OPEN: leave existing row untouched (no re-open spam).
# WHEN NOT MATCHED: insert fresh alert.
# -----------------------------------------------------------------------------
spark.sql(f"""
    MERGE INTO dq_alert d
    USING (
        SELECT
            SHA2(CONCAT_WS('|', v.alert_hash, r.rule_id), 256) AS alert_id,
            v.alert_hash,
            r.rule_id, v.rule_code, v.rule_version, v.category,
            v.Prosess_id, v.Saksnummer, v.Indikator, v.Enhets_id, v.Enhet_navn,
            v.Saksbehandler_kode, v.Saksbehandler_navn,
            v.severity, v.alert_message
        FROM all_case_violations v
        JOIN dq_rule r ON r.rule_code = v.rule_code AND r.active_flag = true
    ) s
    ON  d.alert_hash = s.alert_hash
    AND d.status     = 'OPEN'
    WHEN MATCHED THEN UPDATE SET
        d.alert_message = s.alert_message,
        d.batch_id      = '{BATCH_ID}'
    WHEN NOT MATCHED THEN INSERT (
        alert_id, alert_hash, rule_id, rule_code, rule_version, category,
        prosess_id, saksnummer, indikator, enhets_id, enhet_navn,
        saksbehandler_kode, saksbehandler_navn,
        severity, alert_message,
        detected_at, first_detected_at,
        batch_id, status
    ) VALUES (
        s.alert_id, s.alert_hash,
        s.rule_id, s.rule_code, s.rule_version, s.category,
        s.Prosess_id, s.Saksnummer, s.Indikator, s.Enhets_id, s.Enhet_navn,
        s.Saksbehandler_kode, s.Saksbehandler_navn,
        s.severity, s.alert_message,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
        '{BATCH_ID}', 'OPEN'
    )
""")
print("New alerts upserted.")


# CELL 10 — Auto-resolve alerts that no longer fire
# Delta Lake does not support subqueries in UPDATE WHERE.
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW resolve_case_hashes AS
    SELECT DISTINCT ex.alert_hash
    FROM dq_alert ex
    LEFT ANTI JOIN (
        SELECT DISTINCT alert_hash FROM all_case_violations
    ) cur
    ON ex.alert_hash = cur.alert_hash
    WHERE ex.category = 'CASE'
    AND   ex.status   = 'OPEN'
""")

spark.sql("""
    MERGE INTO dq_alert d
    USING resolve_case_hashes r
    ON  d.alert_hash = r.alert_hash
    AND d.category   = 'CASE'
    AND d.status     = 'OPEN'
    WHEN MATCHED THEN UPDATE SET
        d.status          = 'RESOLVED',
        d.resolved_at     = CURRENT_TIMESTAMP,
        d.resolved_method = 'AUTO_CLEARED'
""")

print("Auto-resolve sweep complete.")


# CELL 11 — Summary
# -----------------------------------------------------------------------------
print("\n=== DQ ALERT SUMMARY ===")
spark.sql("""
    SELECT
        rule_code, severity, status,
        COUNT(*)                              AS n,
        COUNT(DISTINCT saksbehandler_kode)    AS n_handlers,
        COUNT(DISTINCT enhets_id)             AS n_units
    FROM dq_alert
    WHERE category = 'CASE'
    GROUP BY rule_code, severity, status
    ORDER BY rule_code, status
""").show(truncate=False)

print("\n=== OPEN ALERTS BY UNIT ===")
spark.sql("""
    SELECT
        enhet_navn,
        COUNT(*)                                               AS open_alerts,
        SUM(CASE WHEN severity = 'Error'   THEN 1 ELSE 0 END) AS errors,
        SUM(CASE WHEN severity = 'Warning' THEN 1 ELSE 0 END) AS warnings
    FROM dq_alert
    WHERE category = 'CASE'
    AND   status   = 'OPEN'
    GROUP BY enhet_navn
    ORDER BY open_alerts DESC
    LIMIT 20
""").show(truncate=False)
