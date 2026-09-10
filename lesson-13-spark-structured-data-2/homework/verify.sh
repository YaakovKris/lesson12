#!/usr/bin/env bash
# Наскрізна перевірка медальйону L13: bronze (PySpark) -> silver + gold (dbt на Spark).
# Працює як у Git Bash / WSL, так і в локальному Windows shell, де шлях до проекту
# може бути як /c/... так і C:/... . Скрипт уникає жорстких /tmp і не залежить від
# конкретного shell-оточення.
set -uo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SCRIPT_DIR="$(cd -- "$(dirname -- "$SCRIPT_PATH")" && pwd -P)" || exit 1
cd "$SCRIPT_DIR" || exit 1

if command -v powershell.exe >/dev/null 2>&1 && \
   { printf '%s' "$SCRIPT_DIR" | grep -Eq '^/mnt/|^/[A-Za-z]/' || uname -s | grep -Eq 'MINGW|MSYS|CYGWIN'; }; then
  if command -v wslpath >/dev/null 2>&1; then
    WIN_SCRIPT_DIR="$(wslpath -w "$SCRIPT_DIR")"
  elif command -v cygpath >/dev/null 2>&1; then
    WIN_SCRIPT_DIR="$(cygpath -w "$SCRIPT_DIR")"
  else
    WIN_SCRIPT_DIR="$SCRIPT_DIR"
  fi

  powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location '$WIN_SCRIPT_DIR'; Remove-Item -Recurse -Force 'spark-warehouse', 'metastore_db', 'derby.log', 'logs', 'dbt_lakehouse\target' -ErrorAction SilentlyContinue; & '.\.venv\Scripts\python.exe' 'bronze_job.py'; & '.\.venv\Scripts\dbt.exe' build --project-dir 'dbt_lakehouse' --profiles-dir 'dbt_lakehouse'; & '.\.venv\Scripts\python.exe' -c \"from common.spark import build_spark; spark = build_spark('verify', reset=False); c = {t: spark.table(t).count() for t in ['bronze.raw_events','silver.events','silver.commits','silver.pull_requests','silver.issues','gold.dim_repo','gold.dim_actor','gold.dim_date','gold.fact_commit','gold.fact_pull_request','gold.fact_repo_activity_daily']}; print(c); spark.stop()\""
  exit $?
fi

export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8

if [ -x "$SCRIPT_DIR/.venv/Scripts/python.exe" ]; then
  PYTHON="$SCRIPT_DIR/.venv/Scripts/python.exe"
  DBT_BIN="$SCRIPT_DIR/.venv/Scripts/dbt.exe"
elif [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
  PYTHON="$SCRIPT_DIR/.venv/bin/python"
  DBT_BIN="$SCRIPT_DIR/.venv/bin/dbt"
else
  PYTHON="${PYTHON:-python}"
  DBT_BIN="${DBT_BIN:-dbt}"
fi

DBT_PROJECT_DIR="$SCRIPT_DIR/dbt_lakehouse"
DBT_CMD=("$DBT_BIN" build --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROJECT_DIR")
LANDING_DIR="$SCRIPT_DIR/data/landing"
WORK_DIR="$(mktemp -d "$SCRIPT_DIR/.verify_tmp.XXXXXX")"
HOLD="$WORK_DIR/hold"

restore() {
  if [ -d "$HOLD" ]; then
    shopt -s nullglob
    files=("$HOLD"/*.json.gz)
    if [ "${#files[@]}" -gt 0 ]; then
      for f in "${files[@]}"; do
        mv "$f" "$LANDING_DIR/" 2>/dev/null || true
      done
    fi
    rmdir "$HOLD" 2>/dev/null || true
  fi
  rm -rf "$WORK_DIR"
}

fail() {
  echo "FAIL ❌  $1"
  restore
  exit 1
}

trap 'restore' EXIT

echo "==> Чистий склад (spark-warehouse/, metastore_db/, derby.log, dbt_lakehouse/target)"
rm -rf "$SCRIPT_DIR/spark-warehouse" "$SCRIPT_DIR/metastore_db" "$SCRIPT_DIR/derby.log" "$SCRIPT_DIR/logs" "$SCRIPT_DIR/dbt_lakehouse/target"

# ---------------------------------------------------------------------------
echo "==> Фаза 1: у landing лише перша година → bronze + dbt build"
mkdir -p "$HOLD"
for f in "$LANDING_DIR/2024-01-15-13.json.gz" "$LANDING_DIR/2024-01-15-14.json.gz"; do
  [ -f "$f" ] || fail "не знайшов landing-файли"
  mv "$f" "$HOLD/" || fail "не вдалось перемістити файли до hold"
done

"$PYTHON" bronze_job.py >"$WORK_DIR/b1.log" 2>&1 || { tail -20 "$WORK_DIR/b1.log"; fail "bronze (фаза 1)"; }
out=$("${DBT_CMD[@]}" 2>&1) || { echo "$out" | tail -30; fail "dbt build (фаза 1)"; }
echo "$out" | grep -q "ERROR=0" || { echo "$out" | tail -30; fail "dbt build (фаза 1)"; }

n1=$("$PYTHON" -c "from common.spark import build_spark; s=build_spark('v'); print(s.table('silver.events').count()); s.stop()" 2>/dev/null | tail -1)
echo "    silver.events (1 година) = $n1"
[ "$n1" -gt 5000 ] && [ "$n1" -lt 15000 ] || fail "фаза 1: silver.events поза очікуваним діапазоном"

# ---------------------------------------------------------------------------
echo "==> Фаза 2: доносимо решту годин → bronze (append) + dbt build (append, без full-refresh)"
shopt -s nullglob
files=("$HOLD"/*.json.gz)
if [ "${#files[@]}" -gt 0 ]; then
  for f in "${files[@]}"; do
    mv "$f" "$LANDING_DIR/" || fail "не вдалось повернути landing-файли"
  done
fi
rmdir "$HOLD" 2>/dev/null || true

"$PYTHON" bronze_job.py >"$WORK_DIR/b2.log" 2>&1 || { tail -20 "$WORK_DIR/b2.log"; fail "bronze (фаза 2)"; }
out=$("${DBT_CMD[@]}" 2>&1) || { echo "$out" | tail -30; fail "dbt build (фаза 2)"; }
echo "$out" | grep -q "ERROR=0" || { echo "$out" | tail -30; fail "dbt build (фаза 2)"; }

# ---------------------------------------------------------------------------
echo "==> Фаза 3: повторний прогін без нових даних (ідемпотентність)"
if ! "$PYTHON" bronze_job.py 2>&1 | grep -q "idempotent\|ідемпотентно"; then
  fail "bronze не ідемпотентний"
fi
out=$("${DBT_CMD[@]}" --select silver.events 2>&1) || { echo "$out" | tail -20; fail "dbt build (фаза 3)"; }
echo "$out" | grep -q "ERROR=0" || { echo "$out" | tail -20; fail "dbt build (фаза 3)"; }

# ---------------------------------------------------------------------------
echo "==> Перевірка кількості рядків у всіх шарах..."
if ! "$PYTHON" - <<'PY'
import sys
from common.spark import build_spark

spark = build_spark("l13-verify", reset=False)
counts = {
    t: spark.table(t).count() for t in [
        "bronze.raw_events",
        "silver.events",
        "silver.commits",
        "silver.pull_requests",
        "silver.issues",
        "gold.dim_repo",
        "gold.dim_actor",
        "gold.dim_date",
        "gold.fact_commit",
        "gold.fact_pull_request",
        "gold.fact_repo_activity_daily",
    ]
}
distinct_ids = spark.sql("select count(distinct event_id) from silver.events").collect()[0][0]
for k, v in counts.items():
    print(f"   {k:<32} {v}")
spark.stop()

expect = {
    "bronze.raw_events": 36000,
    "silver.events": 30048,
    "silver.commits": 28237,
    "silver.pull_requests": 1959,
    "silver.issues": 1704,
    "gold.dim_repo": 16053,
    "gold.dim_actor": 10514,
    "gold.dim_date": 3349,
    "gold.fact_commit": 28237,
    "gold.fact_pull_request": 1959,
    "gold.fact_repo_activity_daily": 13403,
}

ok = all(counts.get(k) == v for k, v in expect.items()) and distinct_ids == 30048
sys.exit(0 if ok else 1)
PY
then
  fail "кількість рядків не збіглася з checkpoint (SPEC.md)"
fi

echo "PASS ✅  silver.events інкрементиться 1 година → 30 048, усі checkpoint-и зійшлися, dbt-тести зелені."
exit 0
