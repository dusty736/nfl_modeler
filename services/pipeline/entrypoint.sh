#!/usr/bin/env bash
set -euo pipefail

echo "starting weekly pipeline…"
echo "DB_HOST=${DB_HOST:-unset}"
echo "DB_NAME=${DB_NAME:-unset}"
echo "DB_USER=${DB_USER:-unset}"

cd /app

# If you rely on renv at runtime for PATHs, consent; restore happens at build
Rscript etl/R/process_scripts/db_refresh_cloud.R

echo "running python predictions…"
python3 modeling/Python/pregame_predgen_cloud.py --run-id latest --all --to-db
python3 modeling/Python/pregame_total_predgen_cloud.py --run-id latest --all --with-pi --to-db
python3 modeling/Python/pregame_margin_predgen_cloud.py --run-id latest --all --to-db

echo "pipeline completed."
