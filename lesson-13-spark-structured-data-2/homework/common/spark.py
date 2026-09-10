"""Спільний помічник для створення SparkSession — ДАНО. Не редагуйте.

Усі кроки (bronze, dbt) працюють з ОДНИМ Hive-metastore і складом таблиць у
`spark-warehouse/` поточної директорії. Тому таблицю, яку записав bronze_job, бачить
dbt (Silver + Gold моделі). Запускайте все з кореня цієї директорії.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import SparkSession


def _reset_local_spark_state(project_root: Path) -> None:
    for stale in [
        project_root / "spark-warehouse",
        project_root / "metastore_db",
        project_root / "derby.log",
        project_root / "logs",
        project_root / "spark-tmp",
    ]:
        if stale.exists():
            if stale.is_dir():
                shutil.rmtree(stale)
            else:
                stale.unlink()


def build_spark(app_name: str, reset: bool = True) -> SparkSession:
    project_root = Path(__file__).resolve().parent.parent
    if reset:
        _reset_local_spark_state(project_root)

    warehouse_dir = project_root / "spark-warehouse"
    metastore_dir = project_root / "metastore_db"
    warehouse_dir.mkdir(exist_ok=True)

    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.warehouse.dir", str(warehouse_dir.resolve()))
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:{metastore_dir.resolve()};create=true")
        .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
        .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "APP")
        .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "mine")
        .config("spark.hadoop.datanucleus.autoCreateSchema", "true")
        .config("spark.hadoop.datanucleus.fixedDatastore", "true")
        .config("spark.hadoop.datanucleus.schema.autoCreateAll", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
