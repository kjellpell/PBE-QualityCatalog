# =============================================================================
# NB_03_CAPACITY_RISK.py
# Computes weekly production metrics per unit.
# Detects units trending toward capacity ceiling.
# Writes to capacity_risk_signal Delta table.
# Schedule: nightly.
# =============================================================================

# CELL 1 — Imports
# -----------------------------------------------------------------------------
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime, date, timedelta

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")

BATCH_ID    = datetime.now().strftime("%Y-%m-%d-capacity")
WEEKS_BACK  = 52
TODAY       = date.today()
WINDOW_FROM = str(TODAY - timedelta(weeks=WEEKS_BACK))

print(f"Batch: {BATCH_ID}")
print(f"Analysis window: {WINDOW_FROM} to {TODAY}")


# CELL 1b — Load source tables
# -----------------------------------------------------------------------------
spark.read.table("Saksbehandling.Prosesser")\
     .createOrReplaceTempView("Prosesser")
spark.read.table("Saksbehandling.Saker")\
     .createOrReplaceTempView("Saker")
spark.read.table("Felles.Organisasjon")\
     .createOrReplaceTempView("Organisasjon")

print("Source tables loaded.")


# CELL 2 — Diagnose unit coverage before anything else
# -----------------------------------------------------------------------------
total_p = spark.sql(f"""
    SELECT COUNT(*) FROM Prosesser
    WHERE tidligste_startmilepael_dato >= '{WINDOW_FROM}'
    AND   tidligste_startmilepael_dato IS NOT NULL
""").collect()[0][0]

matched = spark.sql(f"""
    SELECT COUNT(*) FROM Prosesser p
    JOIN Organisasjon o ON p.Enhets_id = o.Enhets_id
    WHERE p.tidligste_startmilepael_dato >= '{WINDOW_FROM}'
    AND   p.tidligste_startmilepael_dato IS NOT NULL
""").collect()[0][0]

print(f"Prosesser in window: {total_p:,}")
print(f"Matched to Organisasjon: {matched:,} ({matched*100//total_p if total_p else 0}%)")

# Show sample of unmatched Enhets_id values
print("\nSample unmatched Enhets_id:")
spark.sql(f"""
    SELECT DISTINCT p.Enhets_id, p.Enhetskode, COUNT(*) AS n
    FROM Prosesser p
    LEFT JOIN Organisasjon o ON p.Enhets_id = o.Enhets_id
    WHERE p.tidligste_startmilepael_dato >= '{WINDOW_FROM}'
    AND   p.tidligste_startmilepael_dato IS NOT NULL
    AND   o.Enhets_id IS NULL
    GROUP BY p.Enhets_id, p.Enhetskode
    ORDER BY n DESC
    LIMIT 10
""").show(truncate=False)


# CELL 3 — Build base with Monday week calculation in PySpark
# Using PySpark avoids Spark SQL DAYOFWEEK locale issues.
# DAYOFWEEK: 1=Sunday. Monday = day 2.
# To get Monday: date - (dayofweek - 2) if dayofweek >= 2
#                date - (dayofweek + 5) if dayofweek == 1 (Sunday)
# Simpler: next_day of the date 7 days ago = Monday of that week
# Even simpler: use date_trunc on a Tuesday-based offset
# Most reliable: subtract (dayofweek % 7 - 1) days
# -----------------------------------------------------------------------------
p_df = spark.sql(f"""
    SELECT
        p.Prosess_id,
        p.Saksnummer,
        p.Indikator,
        p.Enhets_id,
        p.Enhetskode,
        p.tidligste_startmilepael_dato,
        p.seneste_stoppmilepael_dato,
        p.Tidligbehandling_dato,
        p.innenfor_frist,
        DATEDIFF(p.seneste_stoppmilepael_dato,
            p.tidligste_startmilepael_dato) AS behandlingstid_dager,
        DATEDIFF(p.Tidligbehandling_dato,
            p.tidligste_startmilepael_dato) AS tidlig_dager,
        s.Status AS sak_status,
        COALESCE(o.Enhet_navn, p.Enhetskode,
            CAST(p.Enhets_id AS STRING))    AS Enhet_navn,
        COALESCE(o.Seksjon_navn, '')        AS Seksjon_navn,
        COALESCE(o.Avdeling_navn, '')       AS Avdeling_navn
    FROM Prosesser p
    LEFT JOIN Organisasjon o ON p.Enhets_id = o.Enhets_id
    LEFT JOIN Saker s ON p.Saksnummer = s.Saksnummer
    WHERE p.tidligste_startmilepael_dato IS NOT NULL
    AND   p.tidligste_startmilepael_dato >= '{WINDOW_FROM}'
""")

# Compute Monday of week using PySpark
# DAYOFWEEK returns 1=Sun, 2=Mon, ..., 7=Sat
# Days to subtract to reach Monday: (dayofweek - 2) % 7
def to_monday(col):
    dow = F.dayofweek(col)  # 1=Sun, 2=Mon, ..., 7=Sat
    days_to_monday = ((dow - 2) % 7).cast("int")
    return F.date_sub(col, days_to_monday)

p_df = p_df\
    .withColumn("mottatt_uke",
        to_monday(F.col("tidligste_startmilepael_dato")))\
    .withColumn("produsert_uke",
        F.when(F.col("seneste_stoppmilepael_dato").isNotNull(),
               to_monday(F.col("seneste_stoppmilepael_dato"))
        ).otherwise(None))

p_df.createOrReplaceTempView("base_weekly")

# Verify week dates
print("\nSample week dates (should all be Mondays):")
spark.sql("""
    SELECT
        tidligste_startmilepael_dato,
        mottatt_uke,
        DAYOFWEEK(mottatt_uke) AS dow_check
    FROM base_weekly
    WHERE mottatt_uke IS NOT NULL
    LIMIT 5
""").show()

print(f"\nDistinct units in base: {p_df.select('Enhet_navn').distinct().count()}")
print("Weekly flow computed.")


# CELL 4 — Aggregate to weekly mottatt per unit
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW weekly_mottatt AS
    SELECT
        Enhets_id, Enhet_navn, Seksjon_navn, Avdeling_navn,
        mottatt_uke AS uke_dato,
        COUNT(*) AS mottatt
    FROM base_weekly
    WHERE mottatt_uke IS NOT NULL
    GROUP BY Enhets_id, Enhet_navn, Seksjon_navn, Avdeling_navn, mottatt_uke
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW weekly_produsert AS
    SELECT
        Enhets_id,
        MAX(Enhet_navn)                    AS Enhet_navn,
        MAX(Seksjon_navn)                  AS Seksjon_navn,
        MAX(Avdeling_navn)                 AS Avdeling_navn,
        produsert_uke AS uke_dato,
        COUNT(*)                              AS produsert,
        AVG(behandlingstid_dager)             AS snitt_behandlingstid,
        AVG(CASE WHEN tidlig_dager IS NOT NULL
                 THEN tidlig_dager END)       AS snitt_tidlig_dager,
        SUM(CAST(innenfor_frist AS INT)) * 100.0
            / NULLIF(COUNT(*), 0)             AS pct_innenfor_frist
    FROM base_weekly
    WHERE produsert_uke IS NOT NULL
    GROUP BY Enhets_id, produsert_uke
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW weekly_flow AS
    SELECT
        COALESCE(m.Enhets_id, pr.Enhets_id)       AS Enhets_id,
        COALESCE(m.Enhet_navn, pr.Enhet_navn)     AS Enhet_navn,
        COALESCE(m.Seksjon_navn, pr.Seksjon_navn) AS Seksjon_navn,
        COALESCE(m.Avdeling_navn, pr.Avdeling_navn) AS Avdeling_navn,
        COALESCE(m.uke_dato, pr.uke_dato)         AS uke_dato,
        COALESCE(m.mottatt, 0)                    AS mottatt,
        COALESCE(pr.produsert, 0)                 AS produsert,
        pr.snitt_behandlingstid,
        pr.snitt_tidlig_dager,
        pr.pct_innenfor_frist
    FROM weekly_mottatt m
    FULL OUTER JOIN weekly_produsert pr
        ON  m.Enhets_id = pr.Enhets_id
        AND m.uke_dato  = pr.uke_dato
""")

n_units = spark.sql(
    "SELECT COUNT(DISTINCT Enhet_navn) FROM weekly_flow"
).collect()[0][0]
print(f"Units in weekly flow: {n_units}")
print("Aggregation complete.")


# CELL 5 — Compute cumulative portfolio and 3-week trend
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW weekly_with_portfolio AS
    WITH cumulative AS (
        SELECT
            Enhets_id, Enhet_navn, Seksjon_navn, Avdeling_navn,
            uke_dato, mottatt, produsert,
            snitt_behandlingstid, snitt_tidlig_dager, pct_innenfor_frist,
            SUM(mottatt) OVER (
                PARTITION BY Enhets_id ORDER BY uke_dato
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_mottatt,
            SUM(produsert) OVER (
                PARTITION BY Enhets_id ORDER BY uke_dato
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_produsert,
            CASE WHEN mottatt > 0
                 THEN produsert * 1.0 / mottatt
                 ELSE NULL
            END AS produksjonsrate
        FROM weekly_flow
    )
    SELECT *, cum_mottatt - cum_produsert AS portefolje
    FROM cumulative
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW weekly_with_trend AS
    SELECT
        w.*,
        w.portefolje - LAG(w.portefolje, 3) OVER (
            PARTITION BY w.Enhets_id ORDER BY w.uke_dato
        ) AS portefolje_delta_3u,
        CASE
            WHEN LAG(w.portefolje, 3) OVER (
                PARTITION BY w.Enhets_id ORDER BY w.uke_dato
            ) > 0
            THEN (w.portefolje - LAG(w.portefolje, 3) OVER (
                    PARTITION BY w.Enhets_id ORDER BY w.uke_dato
                 )) * 1.0
                 / LAG(w.portefolje, 3) OVER (
                    PARTITION BY w.Enhets_id ORDER BY w.uke_dato
                 )
            ELSE NULL
        END AS portefolje_trend_3u,
        AVG(w.produksjonsrate) OVER (
            PARTITION BY w.Enhets_id ORDER BY w.uke_dato
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS produksjonsrate_3u_snitt
    FROM weekly_with_portfolio w
""")

print("Portfolio and trend computed.")


# CELL 6 — Risk classification
# -----------------------------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW weekly_with_risk AS
    SELECT *,
        CASE
            WHEN portefolje_trend_3u  > 0.25
            AND  produksjonsrate_3u_snitt < 0.85
            THEN 'CRITICAL'
            WHEN portefolje_trend_3u  > 0.15
            OR   produksjonsrate_3u_snitt < 0.90
            THEN 'HIGH'
            WHEN portefolje_trend_3u  > 0.08
            OR  (snitt_behandlingstid IS NOT NULL
                 AND snitt_behandlingstid > 60)
            THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        CASE
            WHEN portefolje_trend_3u  > 0.25
            AND  produksjonsrate_3u_snitt < 0.85
            THEN CONCAT(
                'Portefølje vokser raskt (',
                CAST(ROUND(portefolje_trend_3u*100,1) AS STRING),
                '% siste 3 uker) og produksjonsrate er lav (',
                CAST(ROUND(produksjonsrate_3u_snitt*100,1) AS STRING),
                '%).')
            WHEN portefolje_trend_3u  > 0.15
            THEN CONCAT(
                'Portefølje vokser (',
                CAST(ROUND(portefolje_trend_3u*100,1) AS STRING),
                '% siste 3 uker). Følges nøye.')
            WHEN produksjonsrate_3u_snitt < 0.90
            THEN CONCAT(
                'Produksjonsrate under 90% siste 3 uker (',
                CAST(ROUND(produksjonsrate_3u_snitt*100,1) AS STRING),
                '%).')
            WHEN snitt_behandlingstid IS NOT NULL
            AND  snitt_behandlingstid > 60
            THEN CONCAT(
                'Snitt behandlingstid ',
                CAST(ROUND(snitt_behandlingstid,0) AS STRING),
                ' dager denne uken.')
            ELSE 'Stabil kapasitet.'
        END AS risk_reason
    FROM weekly_with_trend
""")

print("Risk levels classified.")


# CELL 7 — Write latest week to capacity_risk_signal
# -----------------------------------------------------------------------------
# Use last completed week — not MAX which may be a future date
# today.weekday() returns 0=Monday, so subtracting that gives this Monday
# Then subtract one more week to get last completed Monday
_today = date.today()
_this_monday = _today - timedelta(days=_today.weekday())
latest_week = str(_this_monday - timedelta(weeks=1))
print(f"\nUsing week (last completed Monday): {latest_week}")

spark.sql(f"DELETE FROM capacity_risk_signal WHERE uke_dato = '{latest_week}'")

spark.sql(f"""
    INSERT INTO capacity_risk_signal (
        signal_id,
        enhets_id, enhet_navn, seksjon_navn, avdeling_navn,
        uke_dato, mottatt, produsert, portefolje,
        produksjonsrate, portefolje_trend_3u,
        snitt_behandlingstid, snitt_tidlig_dager, pct_innenfor_frist,
        risk_level, risk_reason, computed_at, batch_id
    )
    SELECT
        SHA2(CONCAT_WS('|', CAST(Enhets_id AS STRING), CAST(uke_dato AS STRING), '{BATCH_ID}'), 256),
        Enhets_id, Enhet_navn, Seksjon_navn, Avdeling_navn,
        uke_dato, mottatt, produsert, portefolje,
        produksjonsrate, portefolje_trend_3u,
        snitt_behandlingstid, snitt_tidlig_dager, pct_innenfor_frist,
        risk_level, risk_reason, CURRENT_TIMESTAMP, '{BATCH_ID}'
    FROM weekly_with_risk
    WHERE uke_dato = '{latest_week}'
""")

print(f"capacity_risk_signal updated for week: {latest_week}")


# CELL 8 — Summary
# -----------------------------------------------------------------------------
print("\n=== CAPACITY RISK SUMMARY — LATEST WEEK ===")
spark.sql(f"""
    SELECT
        enhet_navn,
        mottatt,
        produsert,
        portefolje,
        ROUND(produksjonsrate * 100, 1)     AS rate_pct,
        ROUND(portefolje_trend_3u * 100, 1) AS trend_3u_pct,
        risk_level
    FROM capacity_risk_signal
    WHERE uke_dato = '{latest_week}'
    ORDER BY
        CASE risk_level
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH'     THEN 2
            WHEN 'MEDIUM'   THEN 3
            ELSE 4
        END,
        portefolje DESC
""").show(50, truncate=False)


# =============================================================================
# CAPACITY FORECAST EXTENSION
# Fits linear trend per unit on weekly mottatt/produsert time series.
# Projects portfolio 6-8 weeks forward.
# Flags units where projected portfolio crosses threshold.
# Appends forecast rows to capacity_risk_signal with uke_dato in the future.
# =============================================================================

# CELL F1 — Forecasting config
# Uses Spark-native trend fitting (no pandas/numpy dependency)
# -----------------------------------------------------------------------------
print("\n=== CAPACITY FORECAST ===")

FORECAST_WEEKS   = 8
TREND_WEEKS      = 26   # weeks of history to fit trend on
ALERT_MULTIPLIER = 1.3  # flag if projected portfolio > current * this factor
USE_SEASONAL_BLEND    = True
SEASONAL_MIN_WEEKS    = 52
SEASONAL_TREND_WEIGHT = 0.6
SEASONAL_PROFILE_WEIGHT = 0.4

if USE_SEASONAL_BLEND:
    _w_sum = SEASONAL_TREND_WEIGHT + SEASONAL_PROFILE_WEIGHT
    if abs(_w_sum - 1.0) > 1e-9 and _w_sum > 0:
        SEASONAL_TREND_WEIGHT = SEASONAL_TREND_WEIGHT / _w_sum
        SEASONAL_PROFILE_WEIGHT = SEASONAL_PROFILE_WEIGHT / _w_sum
        print(
            f"Adjusted seasonal weights to sum to 1.0: "
            f"{SEASONAL_TREND_WEIGHT:.4f}/{SEASONAL_PROFILE_WEIGHT:.4f}"
        )

print(
    f"Seasonal blend enabled: {USE_SEASONAL_BLEND} "
    f"(weights trend/profile: {SEASONAL_TREND_WEIGHT}/{SEASONAL_PROFILE_WEIGHT})"
)


# CELL F2 — Load historical weekly series for trend fitting
# -----------------------------------------------------------------------------
history_sdf = spark.sql("""
    SELECT
        enhets_id,
        enhet_navn,
        seksjon_navn,
        avdeling_navn,
        uke_dato,
        mottatt,
        produsert,
        portefolje
    FROM capacity_risk_signal
    WHERE uke_dato <= CURRENT_DATE()
""")

hist_count = history_sdf.count()
hist_units = history_sdf.select("enhets_id").distinct().count()
print(f"Historical weeks loaded: {hist_count}")
print(f"Units: {hist_units}")


# CELL F3 — Fit linear trend per unit and project forward
# For each unit:
#   - Take last TREND_WEEKS weeks of mottatt and produsert
#   - Fit linear trend (slope + intercept) to each
#   - Project FORECAST_WEEKS forward
#   - Compute projected cumulative portfolio
#   - Flag if projected portfolio exceeds threshold
# -----------------------------------------------------------------------------
w_desc = Window.partitionBy("enhets_id").orderBy(F.col("uke_dato").desc())
w_asc = Window.partitionBy("enhets_id").orderBy(F.col("uke_dato").asc())

trend_input = (
    history_sdf
    .withColumn("rn_desc", F.row_number().over(w_desc))
    .filter(F.col("rn_desc") <= TREND_WEEKS)
)

trend_indexed = (
    trend_input
    .withColumn("x", F.row_number().over(w_asc) - F.lit(1.0))
    .withColumn("x2", F.col("x") * F.col("x"))
    .withColumn("xy_m", F.col("x") * F.col("mottatt"))
    .withColumn("xy_p", F.col("x") * F.col("produsert"))
)

latest_per_unit = (
    history_sdf
    .withColumn("rn", F.row_number().over(w_desc))
    .filter(F.col("rn") == 1)
    .select(
        "enhets_id",
        "enhet_navn",
        "seksjon_navn",
        "avdeling_navn",
        F.col("portefolje").cast("double").alias("current_portfolio"),
        F.col("uke_dato").alias("last_week")
    )
)

trend_stats = (
    trend_indexed
    .groupBy("enhets_id")
    .agg(
        F.count("*").cast("double").alias("n"),
        F.max("x").alias("x_last"),
        F.sum("x").alias("sum_x"),
        F.sum("x2").alias("sum_x2"),
        F.sum(F.col("mottatt").cast("double")).alias("sum_m"),
        F.sum(F.col("produsert").cast("double")).alias("sum_p"),
        F.sum(F.col("xy_m").cast("double")).alias("sum_xy_m"),
        F.sum(F.col("xy_p").cast("double")).alias("sum_xy_p")
    )
)

history_weeks = (
    history_sdf
    .groupBy("enhets_id")
    .agg(F.count("*").alias("hist_weeks"))
)

seasonal_profile = (
    history_sdf
    .withColumn("fut_week_num", F.weekofyear("uke_dato"))
    .groupBy("enhets_id", "fut_week_num")
    .agg(
        F.avg(F.col("mottatt").cast("double")).alias("seasonal_mottatt"),
        F.avg(F.col("produsert").cast("double")).alias("seasonal_produsert"),
        F.count("*").alias("seasonal_obs")
    )
)

trend_model = (
    trend_stats
    .join(latest_per_unit, on="enhets_id", how="inner")
    .join(history_weeks, on="enhets_id", how="left")
    .withColumn("denom", F.col("n") * F.col("sum_x2") - F.col("sum_x") * F.col("sum_x"))
    .filter((F.col("n") >= 6) & (F.abs(F.col("denom")) > F.lit(1e-9)))
    .withColumn(
        "m_slope",
        (F.col("n") * F.col("sum_xy_m") - F.col("sum_x") * F.col("sum_m")) / F.col("denom")
    )
    .withColumn(
        "p_slope",
        (F.col("n") * F.col("sum_xy_p") - F.col("sum_x") * F.col("sum_p")) / F.col("denom")
    )
    .withColumn("m_intercept", (F.col("sum_m") - F.col("m_slope") * F.col("sum_x")) / F.col("n"))
    .withColumn("p_intercept", (F.col("sum_p") - F.col("p_slope") * F.col("sum_x")) / F.col("n"))
)

horizon = spark.range(1, FORECAST_WEEKS + 1).select(F.col("id").cast("int").alias("w"))
seasonal_enabled = F.lit(1 if USE_SEASONAL_BLEND else 0)

forecast_base = (
    trend_model
    .crossJoin(horizon)
    .withColumn("x_fut", F.col("x_last") + F.col("w"))
    .withColumn("fut_uke_dato", F.expr("date_add(last_week, w * 7)"))
    .withColumn("fut_week_num", F.weekofyear("fut_uke_dato"))
    .join(seasonal_profile, on=["enhets_id", "fut_week_num"], how="left")
    .withColumn(
        "proj_mottatt_raw",
        F.greatest(F.lit(0.0), F.col("m_slope") * F.col("x_fut") + F.col("m_intercept"))
    )
    .withColumn(
        "proj_produsert_raw",
        F.greatest(F.lit(0.0), F.col("p_slope") * F.col("x_fut") + F.col("p_intercept"))
    )
    .withColumn(
        "proj_mottatt",
        F.when(
            (seasonal_enabled == 1)
            & (F.col("hist_weeks") >= SEASONAL_MIN_WEEKS)
            & (F.col("seasonal_obs") >= 1),
            F.greatest(
                F.lit(0.0),
                F.lit(SEASONAL_TREND_WEIGHT) * F.col("proj_mottatt_raw")
                + F.lit(SEASONAL_PROFILE_WEIGHT) * F.col("seasonal_mottatt")
            )
        ).otherwise(F.col("proj_mottatt_raw"))
    )
    .withColumn(
        "proj_produsert",
        F.when(
            (seasonal_enabled == 1)
            & (F.col("hist_weeks") >= SEASONAL_MIN_WEEKS)
            & (F.col("seasonal_obs") >= 1),
            F.greatest(
                F.lit(0.0),
                F.lit(SEASONAL_TREND_WEIGHT) * F.col("proj_produsert_raw")
                + F.lit(SEASONAL_PROFILE_WEIGHT) * F.col("seasonal_produsert")
            )
        ).otherwise(F.col("proj_produsert_raw"))
    )
    .withColumn("delta", F.col("proj_mottatt") - F.col("proj_produsert"))
)

w_forecast = Window.partitionBy("enhets_id").orderBy("w")\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

forecast_spark = (
    forecast_base
    .withColumn("cum_delta", F.sum("delta").over(w_forecast))
    .withColumn(
        "projected_portfolio",
        F.greatest(F.lit(0.0), F.col("current_portfolio") + F.col("cum_delta"))
    )
    .withColumn("will_breach", F.col("projected_portfolio") > F.col("current_portfolio") * F.lit(ALERT_MULTIPLIER))
    .withColumn(
        "risk_level",
        F.when(F.col("projected_portfolio") > F.col("current_portfolio") * F.lit(1.5), F.lit("CRITICAL"))
         .when(F.col("will_breach"), F.lit("HIGH"))
         .when(F.col("projected_portfolio") > F.col("current_portfolio") * F.lit(1.1), F.lit("MEDIUM"))
         .otherwise(F.lit("LOW"))
    )
    .withColumn("uke_dato", F.col("fut_uke_dato"))
    .withColumn(
        "risk_reason",
        F.concat(
            F.lit("Prognose uke +"),
            F.col("w").cast("string"),
            F.lit(": portefolje "),
            F.round(F.col("projected_portfolio"), 0).cast("int").cast("string"),
            F.lit(" ("),
            F.when(F.col("projected_portfolio") > F.col("current_portfolio"), F.lit("up"))
             .when(F.col("projected_portfolio") < F.col("current_portfolio"), F.lit("down"))
             .otherwise(F.lit("flat")),
            F.lit(" fra "),
            F.round(F.col("current_portfolio"), 0).cast("int").cast("string"),
            F.lit(").")
        )
    )
    .select(
        F.col("enhets_id").cast("long").alias("enhets_id"),
        "enhet_navn",
        "seksjon_navn",
        "avdeling_navn",
        F.col("uke_dato").cast("date").alias("uke_dato"),
        F.round("proj_mottatt", 0).cast("int").alias("mottatt"),
        F.round("proj_produsert", 0).cast("int").alias("produsert"),
        F.round("projected_portfolio", 0).cast("int").alias("portefolje"),
        F.when(F.col("proj_mottatt") > 0, F.col("proj_produsert") / F.col("proj_mottatt"))
         .otherwise(F.lit(None).cast("double")).alias("produksjonsrate"),
        F.lit(None).cast("double").alias("portefolje_trend_3u"),
        F.lit(None).cast("double").alias("snitt_behandlingstid"),
        F.lit(None).cast("double").alias("snitt_tidlig_dager"),
        F.lit(None).cast("double").alias("pct_innenfor_frist"),
        "risk_level",
        "risk_reason"
    )
)

forecast_count = forecast_spark.count()
print(f"Forecast rows generated: {forecast_count}")


# CELL F4 — Write forecast rows to capacity_risk_signal
# Forecast rows have future uke_dato values — easy to distinguish from actuals.
# Delete previous forecasts before inserting new ones.
# -----------------------------------------------------------------------------
if forecast_count > 0:
    # Delete previous forecast rows (future dates)
    spark.sql(f"DELETE FROM capacity_risk_signal WHERE uke_dato > '{latest_week}'")

    forecast_spark\
        .withColumn("signal_id",
            F.sha2(F.concat_ws("|",
                F.col("enhets_id").cast("string"),
                F.col("uke_dato").cast("string"),
                F.lit(BATCH_ID + "-forecast")
            ), 256)
        )\
        .withColumn("computed_at", F.current_timestamp())\
        .withColumn("batch_id",    F.lit(BATCH_ID + "-forecast"))\
        .write.mode("append").saveAsTable("capacity_risk_signal")

    print(f"Forecast written for {FORECAST_WEEKS} weeks ahead.")

    # Show forecast summary — units at HIGH or CRITICAL in any forecast week
    print("\n=== FORECAST ALERTS — UNITS AT RISK ===")
    spark.sql(f"""
        SELECT
            enhet_navn,
            MIN(uke_dato)    AS forste_risiko_uke,
            MAX(portefolje)  AS maks_portefolje,
            CASE
                WHEN MAX(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END) = 1
                THEN 'CRITICAL'
                ELSE 'HIGH'
            END AS max_risiko
        FROM capacity_risk_signal
        WHERE uke_dato > '{latest_week}'
        AND   risk_level IN ('HIGH', 'CRITICAL')
        GROUP BY enhet_navn
        ORDER BY maks_portefolje DESC
    """).show(20, truncate=False)
