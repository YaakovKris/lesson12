"""PySpark job над GitHub Archive — ВАШ код (L12). Специфікація: SPEC.md.

Реалізуйте функції з `raise NotImplementedError`. Оркестрація (`build_spark`,
`read_raw`, `main`) вже готова — вона викликає ваші функції
і пише результати у data/output/.

Запуск:    uv run python job.py
Перевірка: uv run pytest

Запускайте з кореня homework/ (усі шляхи відносні до нього).
"""

from __future__ import annotations

import logging
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: F401  (знадобиться у ваших функціях)
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window  # noqa: F401  (для top_repos_per_type)

LANDING_GLOB = "data/landing/*.json.gz"
OUTPUT_DIR = "data/output"

TARGET_EVENT_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "IssueCommentEvent",
]
SUMMARY_DIMENSIONS = ["event_type", "repo_owner", "actor_login", "hour"]
TOP_N = 5
BOT_SUFFIX = "[bot]"

log = logging.getLogger(__name__)


# ── Крок 1 — схема читання ────────────────────────────────────────────────────
def event_schema() -> StructType:
    """Явна схема landing-файлів (schema-on-read, без inferSchema).

    SPEC.md → «Крок 1».
    """
    return StructType(
        [
            StructField("id", StringType()),
            StructField("type", StringType()),
            StructField("actor", StructType([StructField("login", StringType())])),
            StructField("repo", StructType([StructField("name", StringType())])),
            StructField("public", BooleanType()),
            StructField("created_at", StringType()),
        ]
    )


def read_raw(spark: SparkSession) -> DataFrame:
    """ДАНО. Подає вашу схему у reader — жодного inferSchema."""
    return spark.read.schema(event_schema()).json(LANDING_GLOB)


# ── Крок 2 — сплющення ────────────────────────────────────────────────────────
def flatten(raw: DataFrame) -> DataFrame:
    """Розгорнути вкладені структури у пласкі колонки. SPEC.md → «Крок 2»."""
    return raw.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.name").alias("repo_name"),
        F.col("public"),
        F.to_timestamp("created_at").alias("created_at"),
    )


# ── Крок 3 — очищення ─────────────────────────────────────────────────────────
def clean(events: DataFrame) -> DataFrame:
    """Фільтри якості + дедуплікація. SPEC.md → «Крок 3»."""
    return (
        events.filter(
            F.col("event_type").isin(TARGET_EVENT_TYPES)
            & (F.col("public") == F.lit(True))
            & F.col("event_id").isNotNull()
            & F.col("repo_name").isNotNull()
            & F.col("created_at").isNotNull()
        )
        .dropDuplicates(["event_id"])
    )


# ── Крок 4 — похідні колонки ──────────────────────────────────────────────────
def with_derived(events: DataFrame) -> DataFrame:
    """Додати repo_owner, is_bot, hour. SPEC.md → «Крок 4»."""
    return (
        events.withColumn("repo_owner", F.split("repo_name", "/").getItem(0))
        .withColumn(
            "is_bot",
            F.coalesce(F.col("actor_login").endswith(BOT_SUFFIX), F.lit(False)),
        )
        .withColumn("hour", F.date_trunc("hour", F.col("created_at")))
    )


# ── Крок 5 — підсумки по власниках ────────────────────────────────────────────
def owner_totals(events: DataFrame) -> DataFrame:
    """Агрегат: один рядок на repo_owner. SPEC.md → «Крок 5»."""
    return events.groupBy("repo_owner").agg(
        F.count(F.lit(1)).alias("owner_events"),
        F.countDistinct("repo_name").alias("owner_repos"),
        F.count(F.when(F.col("is_bot"), F.lit(1))).alias("owner_bot_events"),
    )


# ── Крок 6 — топ-N репозиторіїв у межах типу події ────────────────────────────
def top_repos_per_type(events: DataFrame, n: int) -> DataFrame:
    """Топ-N репозиторіїв усередині кожного event_type. SPEC.md → «Крок 6»."""
    counts = events.groupBy("event_type", "repo_name").agg(
        F.count(F.lit(1)).alias("repo_event_count")
    )
    ranking = Window.partitionBy("event_type").orderBy(
        F.col("repo_event_count").desc(), F.col("repo_name").asc()
    )
    return (
        counts.withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") <= n)
        .select("event_type", "repo_name", "repo_event_count", "rank")
    )


# ── Крок 7 — збагачення топу підсумками власника ──────────────────────────────
def enrich_top_repos(top_repos: DataFrame, owners: DataFrame) -> DataFrame:
    """LEFT JOIN топу з підсумками власників + частка. SPEC.md → «Крок 7»."""
    top = top_repos.withColumn(
        "repo_owner", F.split("repo_name", "/").getItem(0)
    )
    owner_columns = owners.select(
        "repo_owner", "owner_events", "owner_repos"
    ).alias("owners")
    joined = top.alias("top").join(
        F.broadcast(owner_columns), on="repo_owner", how="left"
    )
    owner_events = F.coalesce(F.col("owners.owner_events"), F.lit(0))
    owner_repos = F.coalesce(F.col("owners.owner_repos"), F.lit(0))
    return joined.select(
        "event_type",
        "repo_name",
        "repo_owner",
        "repo_event_count",
        "rank",
        owner_events.alias("owner_events"),
        owner_repos.alias("owner_repos"),
        F.when(
            owner_events != 0,
            F.round(F.col("repo_event_count") / owner_events, 4),
        ).alias("owner_share"),
    )


# ── Крок 8 — один зріз підсумкової таблиці ────────────────────────────────────
def summary_slice(events: DataFrame, dimension: str) -> DataFrame:
    """Один зріз підсумків за виміром, назва якого приходить аргументом.

    SPEC.md → «Крок 8».
    """
    return events.groupBy(F.col(dimension).cast(StringType()).alias("dimension_value")).agg(
        F.count(F.lit(1)).alias("events"),
        F.countDistinct("repo_name").alias("distinct_repos"),
    ).select(
        F.lit(dimension).alias("dimension"),
        "dimension_value",
        "events",
        "distinct_repos",
    )


# ── Крок 9 — усі зрізи в одній таблиці ────────────────────────────────────────
def build_summary(events: DataFrame, dimensions: list[str]) -> DataFrame:
    """Усі зрізи, зібрані в одну таблицю. SPEC.md → «Крок 9»."""
    slices = [summary_slice(events, dimension) for dimension in dimensions]
    summary = slices[0]
    for current_slice in slices[1:]:
        summary = summary.unionByName(current_slice)
    return summary


# ── Крок 10 — запис marts ─────────────────────────────────────────────────────
def write_outputs(outputs: dict[str, tuple[DataFrame, str | None]]) -> None:
    """Записати кожен mart у data/output/<name>/. SPEC.md → «Крок 10»."""
    for name, (df, partition_column) in outputs.items():
        if partition_column is None:
            df.coalesce(1).write.mode("overwrite").parquet(f"{OUTPUT_DIR}/{name}")
        else:
            (
                df.repartition(partition_column)
                .write.mode("overwrite")
                .partitionBy(partition_column)
                .parquet(f"{OUTPUT_DIR}/{name}")
            )


# ── Оркестрація (ДАНО) ────────────────────────────────────────────────────────
def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        # UTC — інакше date_trunc("hour") дасть різні значення на різних машинах
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s"
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)  # інакше py4j засмічує вивід
    spark = build_spark("l12-github")
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

    # events читається кількома marts — тому cache(), а не чотири перечитування landing
    events = with_derived(clean(flatten(read_raw(spark)))).cache()

    owners = owner_totals(events)
    top_repos = enrich_top_repos(top_repos_per_type(events, TOP_N), owners)
    summary = build_summary(events, SUMMARY_DIMENSIONS)

    marts: dict[str, tuple[DataFrame, str | None]] = {
        "events": (events, "event_type"),
        "owner_totals": (owners, None),
        "top_repos": (top_repos, None),
        "summary": (summary, None),
    }
    write_outputs(marts)

    for name, (df, _) in marts.items():
        log.info("%-13s %d", f"{name}:", df.count())

    events.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
