# src/main.py
import argparse
import json
import logging
import sys
from .extract import fetch_page
from .transform import normalize_rows, run_quality_checks
from .load import upsert_merge_bq

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("api2bq")

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--project")
    ap.add_argument("--dataset")
    ap.add_argument("--table")
    return ap.parse_args()

def run():
    args = parse_args()
    cfg = json.load(open(args.config, "r", encoding="utf-8"))

    project = args.project or cfg["gcp_project"]
    dataset = args.dataset or cfg["bq_dataset"]
    table   = args.table   or cfg["bq_table"]

    log.info("extracting…")
    rows = fetch_page(1)

    df = normalize_rows(rows)
    run_quality_checks(df)

    log.info("loading %d rows -> %s.%s.%s", len(df), project, dataset, table)
    upsert_merge_bq(df, project, dataset, table, key_cols=["icao24", "last_contact"])
    log.info("done.")

if __name__ == "__main__":
    try:
        run()
    except Exception:
        log.exception("pipeline failed")
        sys.exit(1)

