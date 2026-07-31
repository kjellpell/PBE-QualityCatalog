"""
Synthetic source tables for the rule catalogs.

Seeded so that every rule in rules/*.yaml has both passing and failing rows,
including the NULL cases that decide evaluation denominators. The same data
serves the pre-change baseline and the post-change run, so the equivalence
diff is apples-to-apples.

Explicit schemas everywhere: Spark cannot infer a type for an all-NULL column.
"""

from datetime import date

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

SOURCE_SCHEMA = "saksbehandling"

_FASER_SCHEMA = StructType([
    StructField("stage_recno", IntegerType()),
    StructField("to_case_recno", IntegerType()),
    StructField("tidligste_startmilepael_dato", DateType()),
    StructField("seneste_stoppmilepael_dato", DateType()),
    StructField("fase_lukket_dato", DateType()),
    StructField("tidsbruk", IntegerType()),
    StructField("bransjetid", IntegerType()),
    StructField("indikator", StringType()),
    StructField("opprinnelig_frist_dager", IntegerType()),
    StructField("frist_dager", IntegerType()),
    StructField("decisiondate", DateType()),
    StructField("fagsystem", StringType()),
])

_SAKER_SCHEMA = StructType([
    StructField("case_recno", IntegerType()),
    StructField("saksnummer", StringType()),
    StructField("saksansvarlig_kode", StringType()),
])

_MILEPAELER_SCHEMA = StructType([
    StructField("milestone_recno", IntegerType()),
    StructField("milestone_title", StringType()),
    StructField("to_stage_recno", IntegerType()),
    StructField("statusdescription", StringType()),
    StructField("milestonedate", DateType()),
    StructField("fagsystem", StringType()),
])

_FAKTURA_SCHEMA = StructType([
    StructField("fakturanr", StringType()),
    StructField("linje_belop", DoubleType()),
    StructField("fakturasum", DoubleType()),
])

_D0 = date(2024, 1, 1)   # default start
_D1 = date(2024, 3, 1)   # default stop
_D2 = date(2024, 3, 2)   # default lukket
_D3 = date(2024, 3, 5)   # default decisiondate


def _fase(stage_recno, **over):
    """A fully-passing faser row; override one field to induce one failure."""
    row = {
        "stage_recno": stage_recno,
        "to_case_recno": 100 + stage_recno,
        "tidligste_startmilepael_dato": _D0,
        "seneste_stoppmilepael_dato": _D1,
        "fase_lukket_dato": _D2,
        "tidsbruk": 10,
        "bransjetid": 5,
        "indikator": "Annet",
        "opprinnelig_frist_dager": 21,
        "frist_dager": 21,
        "decisiondate": _D3,
        "fagsystem": "PB360",
    }
    row.update(over)
    return tuple(row[f.name] for f in _FASER_SCHEMA.fields)


def _faser_rows():
    return [
        _fase(1),                                              # passes everything
        _fase(2, to_case_recno=None),                          # FAS-001 to_case_recno
        _fase(3, tidligste_startmilepael_dato=None),           # FAS-001a / FAS-003
        _fase(4, fase_lukket_dato=None),                       # FAS-003 fase_lukket_dato
        _fase(5, seneste_stoppmilepael_dato=None),             # FAS-004 (open phase)
        _fase(6, seneste_stoppmilepael_dato=date(2023, 12, 1)),  # FAS-005 stop < start
        _fase(7, tidsbruk=-5),                                 # FAS-006
        _fase(8, bransjetid=-2),                               # FAS-007
        _fase(9, indikator="Delesak 3 uker", frist_dager=30),   # FAS-008 mismatch
        _fase(10, indikator="Til politisk behandling", frist_dager=200),  # FAS-009
        _fase(11, decisiondate=date(2024, 2, 1)),              # FAS-010
        _fase(12, fagsystem="ALTINN"),                         # excluded by catalog filter
        _fase(1),                                              # FAS-002 duplicate stage_recno
        _fase(14, indikator="Delesak 3 uker"),                 # FAS-008 passing case
        _fase(15, indikator="Til politisk behandling", frist_dager=100),  # FAS-009 passing
    ]


def _saker_rows():
    # saksansvarlig_kode is joined in; row 5 is the open phase with no handler.
    rows = [(100 + i, f"SAK-{i:03d}", "SB01") for i in range(1, 16)]
    rows = [r if r[0] != 105 else (105, "SAK-005", None) for r in rows]
    return rows


def _milepaeler_rows():
    """
    Dates are spread one day apart within each group so that event_flow's
    ordering is genuinely exercised. With every milestone on the same date the
    ordering check would be vacuous and a regression could slip through.
    """
    G_PASS, G_INCOMPLETE, G_UNGATED, G_MISORDERED = 10, 11, 12, 13

    def day(n):
        return date(2024, 2, n)

    return [
        # Group 10 — gated in, both pairs complete and in order -> passes
        (1, "Sendt til politisk behandling", G_PASS, "Nådd", day(1), "PB360"),
        (2, "Merknader oversendt", G_PASS, "Nådd", day(2), "PB360"),
        (3, "Mottatt revidert planforslag", G_PASS, "Nådd", day(3), "PB360"),
        (4, "Anmodning om oppdatert plandokumentasjon", G_PASS, "Nådd", day(4), "PB360"),
        (5, "Mottatt oppdatert plandokumentasjon", G_PASS, "Nådd", day(5), "PB360"),
        # Group 11 — gated in, pair opened and never closed -> MIL-004 violation
        (6, "Sendt til politisk behandling", G_INCOMPLETE, "Nådd", day(1), "PB360"),
        (7, "Merknader oversendt", G_INCOMPLETE, "Nådd", day(2), "PB360"),
        # Group 12 — never gated in; the same incomplete pair must NOT be reported
        (8, "Merknader oversendt", G_UNGATED, "Nådd", day(2), "PB360"),
        # Group 13 — gated in, both events present but in the wrong order.
        # Only an ordering check catches this; a presence check would pass it.
        (18, "Sendt til politisk behandling", G_MISORDERED, "Nådd", day(1), "PB360"),
        (19, "Mottatt revidert planforslag", G_MISORDERED, "Nådd", day(2), "PB360"),
        (20, "Merknader oversendt", G_MISORDERED, "Nådd", day(3), "PB360"),
        # MIL-001 / MIL-002: required columns null
        (9, None, G_PASS, "Nådd", day(6), "PB360"),
        (10, "Merknader oversendt", None, "Nådd", day(6), "PB360"),
        # MIL-003: reached milestone with no date
        (11, "Merknader oversendt", G_PASS, "Nådd", None, "PB360"),
        # MIL-003 passing counterpart: not reached, no date required
        (12, "Merknader oversendt", G_PASS, "Ikke nådd", None, "PB360"),
        # Excluded by the catalog filter
        (13, None, None, "Nådd", None, "ALTINN"),
        # Gates for MIL-006 / MIL-007, each with one unclosed pair
        (14, "Vedtak høring og offentlig ettersyn", 20, "Nådd", day(1), "PB360"),
        (15, "Anmodning om oppdatert plandokumentasjon", 20, "Nådd", day(2), "PB360"),
        (16, "Innkalling til oppstartsmøte", 30, "Nådd", day(1), "PB360"),
        (17, "Anmodning om oppdatert plandokumentasjon", 30, "Nådd", day(2), "PB360"),
    ]


def _faktura_rows():
    return [
        ("INV-1", 60.0, 100.0),    # INV-1 sums to 100 -> FAK-002 passes
        ("INV-1", 40.0, 100.0),
        ("INV-2", 60.0, 100.0),    # INV-2 sums to 90 -> FAK-002 fails
        ("INV-2", 30.0, 100.0),
        ("INV-3", None, 50.0),     # FAK-001 linje_belop
        (None, 25.0, 25.0),        # FAK-001 fakturanr
    ]


def create_source_tables(spark) -> None:
    """(Re)create the four source tables the rule catalogs read."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SOURCE_SCHEMA}")
    for name, rows, schema in (
        ("faser", _faser_rows(), _FASER_SCHEMA),
        ("saker", _saker_rows(), _SAKER_SCHEMA),
        ("milepaeler", _milepaeler_rows(), _MILEPAELER_SCHEMA),
        ("fakturalinjer", _faktura_rows(), _FAKTURA_SCHEMA),
    ):
        (
            spark.createDataFrame(rows, schema=schema)
            .write.format("delta").mode("overwrite")
            .saveAsTable(f"{SOURCE_SCHEMA}.{name}")
        )


def create_output_tables(spark, schema: str, result_schema, violation_schema, metric_schema) -> None:
    """Create the dq_* output tables (and their _tmp twins) from the engine schemas."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for base, struct in (
        ("dq_run_results", result_schema),
        ("dq_violations", violation_schema),
        ("dq_execution_metrics", metric_schema),
    ):
        for name in (base, f"{base}_tmp"):
            (
                spark.createDataFrame([], schema=struct)
                .write.format("delta").mode("overwrite")
                .saveAsTable(f"{schema}.{name}")
            )
