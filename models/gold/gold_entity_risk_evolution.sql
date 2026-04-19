WITH risk_assessments AS (
    -- Union audit risks and finding risks for a unified risk view per entity
    SELECT
        a.entity_id,
        ar.risk_id,
        ar.risk_name,
        ar.risk_category,
        ar.risk_score,
        ar.assessment_date
    FROM {{ ref('silver_audit_risks') }} ar
    JOIN {{ ref('silver_audits') }}       a ON ar.audit_id = a.audit_id

    UNION ALL

    SELECT
        a.entity_id,
        fr.risk_id,
        fr.risk_name,
        fr.risk_category,
        fr.risk_score,
        fr.assessment_date
    FROM {{ ref('silver_finding_risks') }} fr
    JOIN {{ ref('silver_findings') }}       f  ON fr.finding_id  = f.finding_id
    JOIN {{ ref('silver_audits') }}         a  ON f.audit_id     = a.audit_id
),

windowed AS (
    SELECT
        ra.entity_id,
        AVG(CASE WHEN ra.assessment_date >= CURRENT_DATE - INTERVAL 90  DAY THEN ra.risk_score END) AS risk_score_avg_current,
        AVG(CASE WHEN ra.assessment_date BETWEEN CURRENT_DATE - INTERVAL 180 DAY AND CURRENT_DATE - INTERVAL 90 DAY THEN ra.risk_score END) AS risk_score_avg_3m_ago,
        AVG(CASE WHEN ra.assessment_date BETWEEN CURRENT_DATE - INTERVAL 270 DAY AND CURRENT_DATE - INTERVAL 180 DAY THEN ra.risk_score END) AS risk_score_avg_6m_ago,
        AVG(CASE WHEN ra.assessment_date BETWEEN CURRENT_DATE - INTERVAL 450 DAY AND CURRENT_DATE - INTERVAL 360 DAY THEN ra.risk_score END) AS risk_score_avg_12m_ago,
        COUNT(DISTINCT ra.risk_id)                                                                   AS total_risks,
        COUNT(DISTINCT CASE WHEN ra.risk_score >= 8 THEN ra.risk_id END)                             AS high_risk_count
    FROM risk_assessments ra
    GROUP BY ra.entity_id
)

SELECT
    e.entity_id,
    e.entity_name,
    e.business_unit,
    e.region,
    e.entity_type,
    w.risk_score_avg_current,
    w.risk_score_avg_3m_ago,
    ROUND(w.risk_score_avg_current - w.risk_score_avg_3m_ago, 2)   AS risk_score_change_3m,
    w.risk_score_avg_6m_ago,
    ROUND(w.risk_score_avg_current - w.risk_score_avg_6m_ago, 2)   AS risk_score_change_6m,
    w.risk_score_avg_12m_ago,
    ROUND(w.risk_score_avg_current - w.risk_score_avg_12m_ago, 2)  AS risk_score_change_12m,
    w.total_risks,
    w.high_risk_count,
    COUNT(DISTINCT CASE WHEN f.finding_rag_status = 'Amber' THEN f.finding_id END) AS amber_findings_count,
    COUNT(DISTINCT CASE WHEN f.finding_status = 'Open'      THEN f.finding_id END) AS open_findings_count,
    a_latest.audit_rating                                                            AS latest_audit_rating,
    a_latest.actual_end_date                                                         AS latest_audit_end_date,
    CASE
        WHEN ROUND(w.risk_score_avg_current - w.risk_score_avg_3m_ago, 2) < -0.5 THEN 'Improving'
        WHEN ROUND(w.risk_score_avg_current - w.risk_score_avg_3m_ago, 2) >  0.5 THEN 'Deteriorating'
        ELSE 'Stable'
    END AS risk_trend_direction,
    CURRENT_DATE AS reporting_date
FROM {{ ref('silver_auditable_entities') }}  e
LEFT JOIN windowed w ON e.entity_id = w.entity_id
LEFT JOIN {{ ref('silver_findings') }}   f ON f.audit_id IN (
    SELECT audit_id FROM {{ ref('silver_audits') }} WHERE entity_id = e.entity_id
)
LEFT JOIN LATERAL (
    SELECT audit_rating, actual_end_date
    FROM {{ ref('silver_audits') }}
    WHERE entity_id = e.entity_id
    ORDER BY actual_end_date DESC NULLS LAST
    LIMIT 1
) a_latest ON TRUE
GROUP BY ALL