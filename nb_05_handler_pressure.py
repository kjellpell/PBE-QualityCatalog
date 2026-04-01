# =============================================================================
# NB_05_HANDLER_PRESSURE.py
# Computes handler queue distribution per handler per indicator.
# Builds rolling 52-week personal baseline per handler × indicator.
# Flags handlers whose current load deviates from their own baseline.
# Aggregates to unit level for business team view.
# Falls back to unit × indicator baseline when handler history is sparse.
# Schedule: nightly.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, date, timedelta

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")

BATCH_ID           = datetime.now().strftime("%Y-%m-%d-handler")
BASELINE_WEEKS     = 52
MIN_CASES_HANDLER  = 10
PRESSURE_THRESHOLD = 1.5

today = date.today()
print(f"Batch: {BATCH_ID}")


# ── Output table ──────────────────────────────────────────────────────────────
# No inline comments inside CREATE TABLE — Spark SQL does not support them
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE TABLE IF NOT EXISTS handler_pressure (
        snapshot_date       DATE,
        saksbehandler_kode  STRING,
        saksbehandler_navn  STRING,
        enhets_id           BIGINT,
        enhet_navn          STRING,
        seksjon_navn        STRING,
        indikator           STRING,
        queue_early         INT,
        queue_mid           INT,
        queue_parked        INT,
        queue_late          INT,
        queue_total         INT,
        baseline_avg        DOUBLE,
        baseline_std        DOUBLE,
        baseline_source     STRING,
        z_score             DOUBLE,
        pressure_level      STRING,
        computed_at         TIMESTAMP,
        batch_id            STRING
    )
    USING DELTA
""")
print("Output table ready.")


# ── Load source tables ────────────────────────────────────────────────────────
spark.read.table("Saksbehandling.Prosesser")\
     .createOrReplaceTempView("Prosesser")
spark.read.table("Saksbehandling.Saker")\
     .createOrReplaceTempView("Saker")
spark.read.table("Felles.Organisasjon")\
     .createOrReplaceTempView("Organisasjon")
print("Source tables loaded.")


# ── Pre-join indicator thresholds to avoid correlated subquery ────────────────
# Load max_open_days per indicator from dq_rule_param.
# Global default 365 used where no indicator-specific row exists.
# This avoids a correlated subquery in the is_late CASE expression.
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW indicator_thresholds AS
    SELECT
        i.Indikator,
        COALESCE(
            CAST(rp.param_value AS INT),
            365
        ) AS max_open_days
    FROM (SELECT DISTINCT Indikator FROM Prosesser WHERE Indikator IS NOT NULL) i
    LEFT JOIN dq_rule_param rp
        ON  rp.rule_id    = 3
        AND rp.scope_type = 'INDIKATOR'
        AND rp.scope_key  = i.Indikator
        AND rp.param_name = 'max_open_days'
""")
print("Indicator thresholds loaded.")


# ── Current queue snapshot per handler × indicator ───────────────────────────
# Queue stages (a case can be in multiple simultaneously):
#   early  = received, no tidligbehandling yet
#   mid    = tidligbehandling done, awaiting completion
#   parked = er_mangelfull = 1, waiting for applicant documentation
#   late   = open beyond indicator threshold (from pre-joined thresholds)
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW current_queue AS
    SELECT
        p.Saksbehandler_kode,
        p.Saksbehandler_navn,
        p.Enhets_id,
        COALESCE(o.Enhet_navn, p.Enhetskode,
            CAST(p.Enhets_id AS STRING))    AS Enhet_navn,
        COALESCE(o.Seksjon_navn, '')        AS Seksjon_navn,
        p.Indikator,
        CASE WHEN p.Tidligbehandling_dato IS NULL
             THEN 1 ELSE 0 END             AS is_early,
        CASE WHEN p.Tidligbehandling_dato IS NOT NULL
             THEN 1 ELSE 0 END             AS is_mid,
        CASE WHEN p.Er_mangelfull = 1
             THEN 1 ELSE 0 END             AS is_parked,
        CASE WHEN DATEDIFF(CURRENT_DATE(), p.tidligste_startmilepael_dato)
                  > COALESCE(t.max_open_days, 365)
             THEN 1 ELSE 0 END             AS is_late
    FROM Prosesser p
    LEFT JOIN Organisasjon o
        ON  p.Enhets_id = o.Enhets_id
    LEFT JOIN Saker s
        ON  p.Saksnummer = s.Saksnummer
    LEFT JOIN indicator_thresholds t
        ON  p.Indikator = t.Indikator
    WHERE p.seneste_stoppmilepael_dato IS NULL
    AND   p.tidligste_startmilepael_dato IS NOT NULL
    AND   s.Status <> 'Saken er avsluttet'
    AND   p.Delprosess_navn NOT IN (
        'Planinitiativ (oppstartsfase før RIPP)',
        'Planskisse (dialogfase før RIPP)'
    )
    AND   p.Saksbehandler_kode IS NOT NULL
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW queue_summary AS
    SELECT
        Saksbehandler_kode,
        Saksbehandler_navn,
        Enhets_id,
        Enhet_navn,
        Seksjon_navn,
        Indikator,
        SUM(is_early)   AS queue_early,
        SUM(is_mid)     AS queue_mid,
        SUM(is_parked)  AS queue_parked,
        SUM(is_late)    AS queue_late,
        COUNT(*)        AS queue_total
    FROM current_queue
    GROUP BY
        Saksbehandler_kode, Saksbehandler_navn,
        Enhets_id, Enhet_navn, Seksjon_navn, Indikator
""")

n_handlers = spark.sql(
    "SELECT COUNT(DISTINCT Saksbehandler_kode) FROM queue_summary"
).collect()[0][0]
print(f"Active handlers with open cases: {n_handlers}")


# ── Rolling 52-week portfolio baseline per handler × indicator ────────────────
# Net in/out calculation: cumulative mottatt - cumulative produsert per week.
# Mean and std dev of that rolling series = personal baseline.
# -----------------------------------------------------------------------------
window_from = str(today - timedelta(weeks=BASELINE_WEEKS))

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW weekly_handler_flow AS
    WITH mottatt AS (
        SELECT
            Saksbehandler_kode,
            Indikator,
            DATE_SUB(
                tidligste_startmilepael_dato,
                CAST((DAYOFWEEK(tidligste_startmilepael_dato) - 2 + 7) % 7 AS INT)
            ) AS uke_dato,
            COUNT(*) AS mottatt
        FROM Prosesser
        WHERE tidligste_startmilepael_dato >= '{window_from}'
        AND   tidligste_startmilepael_dato IS NOT NULL
        AND   Saksbehandler_kode IS NOT NULL
        GROUP BY
            Saksbehandler_kode, Indikator,
            DATE_SUB(
                tidligste_startmilepael_dato,
                CAST((DAYOFWEEK(tidligste_startmilepael_dato) - 2 + 7) % 7 AS INT)
            )
    ),
    produsert AS (
        SELECT
            Saksbehandler_kode,
            Indikator,
            DATE_SUB(
                seneste_stoppmilepael_dato,
                CAST((DAYOFWEEK(seneste_stoppmilepael_dato) - 2 + 7) % 7 AS INT)
            ) AS uke_dato,
            COUNT(*) AS produsert
        FROM Prosesser
        WHERE seneste_stoppmilepael_dato >= '{window_from}'
        AND   seneste_stoppmilepael_dato IS NOT NULL
        AND   Saksbehandler_kode IS NOT NULL
        GROUP BY
            Saksbehandler_kode, Indikator,
            DATE_SUB(
                seneste_stoppmilepael_dato,
                CAST((DAYOFWEEK(seneste_stoppmilepael_dato) - 2 + 7) % 7 AS INT)
            )
    )
    SELECT
        COALESCE(m.Saksbehandler_kode, p.Saksbehandler_kode) AS Saksbehandler_kode,
        COALESCE(m.Indikator, p.Indikator)                   AS Indikator,
        COALESCE(m.uke_dato, p.uke_dato)                     AS uke_dato,
        COALESCE(m.mottatt, 0)                               AS mottatt,
        COALESCE(p.produsert, 0)                             AS produsert,
        SUM(COALESCE(m.mottatt, 0) - COALESCE(p.produsert, 0)) OVER (
            PARTITION BY COALESCE(m.Saksbehandler_kode, p.Saksbehandler_kode),
                         COALESCE(m.Indikator, p.Indikator)
            ORDER BY COALESCE(m.uke_dato, p.uke_dato)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS rolling_portfolio
    FROM mottatt m
    FULL OUTER JOIN produsert p
        ON  m.Saksbehandler_kode = p.Saksbehandler_kode
        AND m.Indikator          = p.Indikator
        AND m.uke_dato           = p.uke_dato
""")

# Handler-level baseline
spark.sql("""
    CREATE OR REPLACE TEMP VIEW handler_baseline AS
    SELECT
        Saksbehandler_kode,
        Indikator,
        COUNT(*)                      AS n_weeks,
        SUM(mottatt + produsert)      AS total_cases,
        AVG(rolling_portfolio)        AS baseline_avg,
        STDDEV(rolling_portfolio)     AS baseline_std
    FROM weekly_handler_flow
    GROUP BY Saksbehandler_kode, Indikator
""")

# Unit-level baseline as fallback — aggregate directly from flow table
# Join Organisasjon to get Enhets_id per handler via Prosesser
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW unit_baseline AS
    SELECT
        p.Enhets_id,
        w.Indikator,
        AVG(w.rolling_portfolio)      AS unit_baseline_avg,
        STDDEV(w.rolling_portfolio)   AS unit_baseline_std
    FROM weekly_handler_flow w
    JOIN (
        SELECT DISTINCT Saksbehandler_kode, Enhets_id
        FROM Prosesser
        WHERE tidligste_startmilepael_dato >= '{window_from}'
    ) p ON w.Saksbehandler_kode = p.Saksbehandler_kode
    GROUP BY p.Enhets_id, w.Indikator
""")

print("Baselines computed.")


# ── Join queue to baseline and compute pressure ───────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW pressure_scored AS
    SELECT
        q.Saksbehandler_kode,
        q.Saksbehandler_navn,
        q.Enhets_id,
        q.Enhet_navn,
        q.Seksjon_navn,
        q.Indikator,
        q.queue_early,
        q.queue_mid,
        q.queue_parked,
        q.queue_late,
        q.queue_total,
        CASE
            WHEN hb.total_cases >= {MIN_CASES_HANDLER}
            THEN hb.baseline_avg
            ELSE ub.unit_baseline_avg
        END AS baseline_avg,
        CASE
            WHEN hb.total_cases >= {MIN_CASES_HANDLER}
            THEN hb.baseline_std
            ELSE ub.unit_baseline_std
        END AS baseline_std,
        CASE
            WHEN hb.total_cases >= {MIN_CASES_HANDLER}
            THEN 'HANDLER'
            ELSE 'UNIT'
        END AS baseline_source,
        CASE
            WHEN hb.total_cases >= {MIN_CASES_HANDLER}
                 AND hb.baseline_std > 0
            THEN (q.queue_total - hb.baseline_avg) / hb.baseline_std
            WHEN ub.unit_baseline_std > 0
            THEN (q.queue_total - ub.unit_baseline_avg) / ub.unit_baseline_std
            ELSE 0.0
        END AS z_score
    FROM queue_summary q
    LEFT JOIN handler_baseline hb
        ON  q.Saksbehandler_kode = hb.Saksbehandler_kode
        AND q.Indikator          = hb.Indikator
    LEFT JOIN unit_baseline ub
        ON  q.Enhets_id = ub.Enhets_id
        AND q.Indikator = ub.Indikator
""")

# Classify pressure level in PySpark — avoids another SQL pass
pressure_df = spark.sql("SELECT * FROM pressure_scored")\
    .withColumn(
        "pressure_level",
        F.when(F.col("z_score") >= 3.0,              "CRITICAL")
         .when(F.col("z_score") >= PRESSURE_THRESHOLD, "HIGH")
         .when(F.col("z_score") >= 0.8,              "MEDIUM")
         .otherwise("LOW")
    )

print(f"Pressure rows: {pressure_df.count():,}")


# ── Write to handler_pressure table ──────────────────────────────────────────
spark.sql(f"DELETE FROM handler_pressure WHERE snapshot_date = '{today}'")

pressure_df\
    .withColumn("snapshot_date", F.lit(str(today)).cast("date"))\
    .withColumn("computed_at",   F.current_timestamp())\
    .withColumn("batch_id",      F.lit(BATCH_ID))\
    .write.mode("append").saveAsTable("handler_pressure")

print(f"handler_pressure written for {today}.")


# ── Summary ───────────────────────────────────────────────────────────────────
print("\n=== HANDLER PRESSURE — HIGH AND CRITICAL ===")
spark.sql(f"""
    SELECT
        enhet_navn,
        saksbehandler_navn,
        indikator,
        queue_total,
        ROUND(baseline_avg, 1)  AS baseline_snitt,
        ROUND(z_score, 1)       AS z,
        pressure_level,
        baseline_source
    FROM handler_pressure
    WHERE snapshot_date = '{today}'
    AND   pressure_level IN ('HIGH', 'CRITICAL')
    ORDER BY z_score DESC
    LIMIT 30
""").show(30, truncate=False)

print("\n=== UNIT PRESSURE AGGREGATION ===")
spark.sql(f"""
    SELECT
        enhet_navn,
        COUNT(DISTINCT saksbehandler_kode)              AS n_handlers,
        SUM(CASE WHEN pressure_level IN ('HIGH','CRITICAL')
                 THEN 1 ELSE 0 END)                     AS n_under_press,
        ROUND(
            SUM(CASE WHEN pressure_level IN ('HIGH','CRITICAL')
                     THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(DISTINCT saksbehandler_kode), 0),
            0
        )                                               AS pct_under_press,
        MAX(ROUND(z_score, 1))                          AS max_z
    FROM handler_pressure
    WHERE snapshot_date = '{today}'
    GROUP BY enhet_navn
    ORDER BY pct_under_press DESC, max_z DESC
""").show(20, truncate=False)
