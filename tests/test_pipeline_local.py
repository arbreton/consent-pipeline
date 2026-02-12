"""End-to-end test of the actual Spark job on generated data.

Needs Java + pyspark (CI installs both); skipped anywhere else. Runs the
real databricks/consent_pipeline.py in a subprocess with a bounded
availableNow run, then asserts bronze/silver/quarantine/gold row counts.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

if shutil.which("java") is None:
    pytest.skip("Java not available — Spark test runs in CI", allow_module_level=True)

REPO = Path(__file__).resolve().parents[1]


def test_job_end_to_end(tmp_path):
    env = dict(
        os.environ,
        SOURCE="local",
        FORMAT="parquet",
        TRIGGER_ONCE="1",
        INPUT_PATH=str(tmp_path / "in"),
        TABLES=str(tmp_path / "tables"),
        CHECKPOINT=str(tmp_path / "chk"),
        PYTHONPATH=str(REPO),
    )
    gen = subprocess.run(
        [sys.executable, "generator/produce_events.py", "--count", "200",
         "--out", str(tmp_path / "in")],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    assert gen.returncode == 0, gen.stderr

    job = subprocess.run(
        [sys.executable, "databricks/consent_pipeline.py"],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert job.returncode == 0, job.stderr[-3000:]

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("consent-pipeline-test")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    bronze = spark.read.parquet(str(tmp_path / "tables" / "bronze")).count()
    silver = spark.read.parquet(str(tmp_path / "tables" / "silver")).count()
    quar = spark.read.parquet(str(tmp_path / "tables" / "quarantine")).count()
    gold = spark.read.parquet(str(tmp_path / "tables" / "gold")).count()
    users = spark.read.parquet(str(tmp_path / "tables" / "silver")).select(
        "user_id"
    ).distinct().count()
    spark.stop()

    assert bronze == 200, "bronze must hold every input row"
    assert silver + quar == bronze, "no row silently dropped"
    assert quar > 0, "bad rows must land in quarantine"
    assert gold == users, "gold holds one row per user"
    assert gold > 0
