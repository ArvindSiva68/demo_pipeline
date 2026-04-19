WITH raw AS (
    SELECT
        finding_id,
        audit_id,
        TRIM(finding_title)                          AS finding_title,
        TRIM(finding_description)                    AS finding_description,
        UPPER(TRIM(finding_status))                  AS finding_status_raw,
        UPPER(TRIM(finding_severity))                AS finding_severity_raw,
        UPPER(TRIM(finding_rag_status))              AS finding_rag_status_raw,
        TRY_CAST(opened_date AS DATE)                AS opened_date,
        TRY_CAST(due_date AS DATE)                   AS due_date,
        TRY_CAST(closed_date AS DATE)                AS closed_date,
        TRIM(owner)                                  AS owner,
        TRY_CAST(created_date AS DATE)               AS created_date,
        TRY_CAST(updated_date AS DATE)               AS updated_date,
        _batch_id,
        _load_timestamp
    FROM read_parquet('data/bronze/findings.parquet')
),

normalised AS (
    SELECT
        *,
        -- RAG normalisation: Amber/AMBER/A all map to 'Amber'
        CASE
            WHEN finding_rag_status_raw IN ('AMBER', 'A', 'AMBER FLAG') THEN 'Amber'
            WHEN finding_rag_status_raw IN ('GREEN', 'G')               THEN 'Green'
            WHEN finding_rag_status_raw IN ('RED', 'R')                 THEN 'Red'
            ELSE 'Unknown'
        END AS finding_rag_status,

        CASE
            WHEN finding_status_raw IN ('OPEN')          THEN 'Open'
            WHEN finding_status_raw IN ('IN PROGRESS')   THEN 'In Progress'
            WHEN finding_status_raw IN ('CLOSED')        THEN 'Closed'
            ELSE 'Unknown'
        END AS finding_status,

        CASE
            WHEN finding_severity_raw = 'CRITICAL' THEN 'Critical'
            WHEN finding_severity_raw = 'HIGH'     THEN 'High'
            WHEN finding_severity_raw = 'MEDIUM'   THEN 'Medium'
            WHEN finding_severity_raw = 'LOW'      THEN 'Low'
            ELSE 'Unknown'
        END AS finding_severity,

        -- Derived fields
        DATEDIFF('day', opened_date, COALESCE(closed_date, CURRENT_DATE)) AS days_open,
        CASE
            WHEN finding_status != 'Closed'
             AND due_date < CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_overdue

    FROM raw
)

-- Only pass records that pass basic validation to the silver layer
SELECT * FROM normalised
WHERE finding_id IS NOT NULL
  AND audit_id   IS NOT NULL
  AND opened_date IS NOT NULL
  AND (closed_date IS NULL OR closed_date >= opened_date)