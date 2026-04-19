import duckdb, os, uuid, yaml
from datetime import datetime

with open("config/pipeline_config.yml") as f:
    cfg = yaml.safe_load(f)

RAW_PATH    = cfg["paths"]["raw"]
BRONZE_PATH = cfg["paths"]["bronze"]
os.makedirs(BRONZE_PATH, exist_ok=True)

SOURCES = [
    "auditable_entities", "audits", "audit_risks",
    "findings", "finding_risks", "auditors"
]

BATCH_ID  = str(uuid.uuid4())
LOAD_TS   = datetime.utcnow().isoformat()

con = duckdb.connect()

audit_log_rows = []

for source in SOURCES:
    src_file = f"{RAW_PATH}/{source}.csv"
    out_file = f"{BRONZE_PATH}/{source}.parquet"

    # Read raw CSV
    df = con.execute(f"SELECT * FROM read_csv_auto('{src_file}', header=true)").df()
    rows_received = len(df)

    # Add governance metadata columns — every bronze record carries these
    df["_source_file"]   = source
    df["_load_timestamp"] = LOAD_TS
    df["_batch_id"]      = BATCH_ID
    df["_source_row_num"] = range(1, len(df) + 1)

    # Write to Parquet (columnar, efficient, preserves types)
    con.execute(f"""
        COPY (SELECT * FROM df)
        TO '{out_file}' (FORMAT PARQUET)
    """)

    audit_log_rows.append({
        "batch_id":       BATCH_ID,
        "load_timestamp": LOAD_TS,
        "source":         source,
        "rows_received":  rows_received,
        "status":         "SUCCESS",
    })
    print(f"[bronze] {source}: {rows_received} rows → {out_file}")

# Write batch audit log
import pandas as pd
log_df = pd.DataFrame(audit_log_rows)
log_path = f"{BRONZE_PATH}/_audit_log.parquet"
if os.path.exists(log_path):
    existing = con.execute(f"SELECT * FROM read_parquet('{log_path}')").df()
    log_df = pd.concat([existing, log_df], ignore_index=True)
con.execute(f"COPY (SELECT * FROM log_df) TO '{log_path}' (FORMAT PARQUET)")

con.close()
print(f"[bronze] Batch {BATCH_ID} complete.")