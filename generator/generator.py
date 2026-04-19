import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
import os, yaml

fake = Faker("en_GB")
random.seed(42)  # base seed for consistency

# --- load config ---
with open("config/pipeline_config.yml") as f:
    cfg = yaml.safe_load(f)

RAW_PATH = cfg["paths"]["raw"]
os.makedirs(RAW_PATH, exist_ok=True)

BATCH_DATE = datetime.utcnow().date()

ENTITY_TYPES   = ["Business Unit", "Process", "Legal Entity", "Technology"]
REGIONS        = ["EMEA", "APAC", "Americas"]
RISK_CATEGORIES = ["Credit", "Market", "Operational", "Compliance", "Technology"]
RISK_RATINGS   = ["Low", "Medium", "High", "Critical"]
AUDIT_RATINGS  = ["Satisfactory", "Needs Improvement", "Unsatisfactory"]
AUDIT_TYPES    = ["Internal", "Regulatory", "Thematic"]
AUDIT_STATUSES = ["Planned", "In Progress", "Completed", "Cancelled"]
FINDING_STATUSES   = ["Open", "In Progress", "Closed"]
FINDING_SEVERITIES = ["Low", "Medium", "High", "Critical"]
FINDING_RAG        = ["Green", "Amber", "Red"]

def random_date(start_days_ago: int, end_days_ago: int = 0) -> str:
    start = datetime.utcnow() - timedelta(days=start_days_ago)
    end   = datetime.utcnow() - timedelta(days=end_days_ago)
    return (start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))).strftime("%Y-%m-%d")

# 1. auditable_entities
n_entities = 30
entities = [{
    "entity_id":    f"ENT-{i:04d}",
    "entity_name":  fake.company(),
    "entity_type":  random.choice(ENTITY_TYPES),
    "business_unit": random.choice(["Front Office", "Risk", "Operations", "Technology", "Compliance"]),
    "region":       random.choice(REGIONS),
    "status":       random.choice(["Active", "Inactive"]),
    "owner":        fake.name(),
    "created_date": random_date(730, 365),
    "updated_date": random_date(30),
} for i in range(1, n_entities + 1)]
pd.DataFrame(entities).to_csv(f"{RAW_PATH}/auditable_entities.csv", index=False)

# 2. auditors
n_auditors = 15
auditors = [{
    "auditor_id":   f"AUD-{i:04d}",
    "auditor_name": fake.name(),
    "email":        fake.email(),
    "team":         random.choice(["Credit Risk", "Market Risk", "Operational Risk", "Compliance"]),
    "manager":      fake.name(),
    "status":       "Active",
} for i in range(1, n_auditors + 1)]
pd.DataFrame(auditors).to_csv(f"{RAW_PATH}/auditors.csv", index=False)

auditor_ids = [a["auditor_id"] for a in auditors]
entity_ids  = [e["entity_id"] for e in entities]

# 3. audits
n_audits = 60
audits_list = []
for i in range(1, n_audits + 1):
    start = random_date(400, 30)
    end   = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=random.randint(14, 90))).strftime("%Y-%m-%d")
    audits_list.append({
        "audit_id":          f"AUDIT-{i:04d}",
        "entity_id":         random.choice(entity_ids),
        "audit_name":        f"{random.choice(['Annual', 'Quarterly', 'Thematic'])} {fake.bs().title()} Review",
        "audit_type":        random.choice(AUDIT_TYPES),
        "audit_status":      random.choice(AUDIT_STATUSES),
        "planned_start_date": start,
        "actual_start_date":  start,
        "actual_end_date":    end,
        "audit_rating":      random.choice(AUDIT_RATINGS),
        "lead_auditor_id":   random.choice(auditor_ids),
        "created_date":      random_date(400, 370),
        "updated_date":      random_date(30),
    })
pd.DataFrame(audits_list).to_csv(f"{RAW_PATH}/audits.csv", index=False)

audit_ids = [a["audit_id"] for a in audits_list]

# 4. findings
n_findings = 120
findings_list = []
for i in range(1, n_findings + 1):
    opened = random_date(365)
    due    = (datetime.strptime(opened, "%Y-%m-%d") + timedelta(days=random.randint(30, 180))).strftime("%Y-%m-%d")
    status = random.choice(FINDING_STATUSES)
    closed = (datetime.strptime(due, "%Y-%m-%d") + timedelta(days=random.randint(-10, 30))).strftime("%Y-%m-%d") if status == "Closed" else None
    findings_list.append({
        "finding_id":          f"FND-{i:04d}",
        "audit_id":            random.choice(audit_ids),
        "finding_title":       fake.sentence(nb_words=6).rstrip("."),
        "finding_description": fake.paragraph(nb_sentences=2),
        "finding_status":      status,
        "finding_severity":    random.choice(FINDING_SEVERITIES),
        "finding_rag_status":  random.choice(FINDING_RAG),
        "opened_date":         opened,
        "due_date":            due,
        "closed_date":         closed,
        "owner":               fake.name(),
        "created_date":        opened,
        "updated_date":        random_date(30),
    })
pd.DataFrame(findings_list).to_csv(f"{RAW_PATH}/findings.csv", index=False)

finding_ids = [f["finding_id"] for f in findings_list]

# 5. audit_risks (link table)
audit_risk_rows = []
for i, audit_id in enumerate(audit_ids):
    for j in range(random.randint(1, 4)):
        assessment_date = random_date(365)
        audit_risk_rows.append({
            "audit_risk_id":    f"AR-{i:04d}-{j:02d}",
            "audit_id":         audit_id,
            "risk_id":          f"RISK-{random.randint(1, 20):04d}",
            "risk_name":        fake.bs().title(),
            "risk_category":    random.choice(RISK_CATEGORIES),
            "risk_rating":      random.choice(RISK_RATINGS),
            "risk_score":       round(random.uniform(1, 10), 2),
            "assessment_date":  assessment_date,
            "created_date":     assessment_date,
            "updated_date":     random_date(30),
        })
pd.DataFrame(audit_risk_rows).to_csv(f"{RAW_PATH}/audit_risks.csv", index=False)

# 6. finding_risks (link table)
finding_risk_rows = []
for i, finding_id in enumerate(finding_ids[:80]):
    assessment_date = random_date(365)
    finding_risk_rows.append({
        "finding_risk_id":  f"FR-{i:04d}",
        "finding_id":       finding_id,
        "risk_id":          f"RISK-{random.randint(1, 20):04d}",
        "risk_name":        fake.bs().title(),
        "risk_category":    random.choice(RISK_CATEGORIES),
        "risk_rating":      random.choice(RISK_RATINGS),
        "risk_score":       round(random.uniform(1, 10), 2),
        "assessment_date":  assessment_date,
        "created_date":     assessment_date,
        "updated_date":     random_date(30),
    })
pd.DataFrame(finding_risk_rows).to_csv(f"{RAW_PATH}/finding_risks.csv", index=False)

print(f"[generator] Batch {BATCH_DATE} — all 6 source files written to {RAW_PATH}/")