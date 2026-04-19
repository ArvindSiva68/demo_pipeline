SELECT
    f.finding_id,
    f.finding_title,
    f.finding_status,
    f.finding_rag_status,
    f.finding_severity,
    f.opened_date,
    f.due_date,
    f.closed_date,
    f.days_open,
    f.is_overdue,
    a.audit_id,
    a.audit_name,
    a.audit_status,
    a.audit_rating,
    e.entity_id,
    e.entity_name,
    e.business_unit,
    e.region,
    au.auditor_name AS lead_auditor_name,
    COUNT(fr.finding_risk_id) AS linked_risk_count
FROM {{ ref('silver_findings') }}     f
JOIN {{ ref('silver_audits') }}       a  ON f.audit_id    = a.audit_id
JOIN {{ ref('silver_auditable_entities') }} e ON a.entity_id = e.entity_id
LEFT JOIN {{ ref('silver_auditors') }} au  ON a.lead_auditor_id = au.auditor_id
LEFT JOIN {{ ref('silver_finding_risks') }} fr ON f.finding_id  = fr.finding_id
WHERE f.finding_rag_status = 'Amber'
GROUP BY ALL