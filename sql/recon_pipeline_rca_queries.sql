/*
Purpose:
  Evidence queries for a reconciliation pipeline delay investigation.

Usage:
  Replace these placeholders before running:
    <<RECONID>>
    <<ENV>>
    <<RECONNAME>>
    <<RECONCILIATION_ID>>

Notes:
  - "This week" is calculated by Oracle using TRUNC(SYSDATE, 'IW').
  - If the incident week differs from the database server's current week,
    replace the bounds CTE with fixed timestamps.
  - These queries only use the columns provided in the investigation request.
*/

/* ============================================================================
   01. Main RCA timeline query
   Compares:
     - Data Merger metadata timestamps
     - Merged Status timestamps
     - Preprocessor/Reconciliation run timestamps
     - Spark/YARN job status rows by JOBID
   ============================================================================ */

WITH
params AS (
    SELECT
        <<RECONID>> AS reconid,
        '<<ENV>>' AS env,
        '<<RECONNAME>>' AS reconname,
        <<RECONCILIATION_ID>> AS reconciliation_id
    FROM dual
),
bounds AS (
    SELECT
        TRUNC(SYSDATE, 'IW') AS week_start,
        TRUNC(SYSDATE, 'IW') + 7 AS week_end
    FROM dual
),
dm AS (
    SELECT
        d.reconid,
        d.triggered_cob_date,
        MIN(d.created_ts) AS dm_created_ts,
        MAX(d.modified_ts) AS dm_modified_ts,
        MAX(d.targetfilename) AS targetfilename,
        MAX(d.filecount) AS dm_filecount,
        MAX(d.recon_sla) AS recon_sla
    FROM fasttrac.om_recon_datamerge d
    JOIN params p ON p.reconid = d.reconid
    CROSS JOIN bounds b
    WHERE (d.created_ts >= b.week_start AND d.created_ts < b.week_end)
       OR (d.modified_ts >= b.week_start AND d.modified_ts < b.week_end)
    GROUP BY d.reconid, d.triggered_cob_date
),
ms AS (
    SELECT
        m.reconid,
        m.env,
        m.triggered_cob_date,
        m.merge_feed_id,
        MAX(m.file_count) AS merged_file_count,
        MAX(m.loaded_count) AS loaded_count,
        MAX(m.issla_breached) AS issla_breached,
        MAX(m.ismerged) AS ismerged,
        MIN(m.merged_ts) AS merged_ts,
        MAX(m.modified_ts) AS merged_status_modified_ts
    FROM fasttrac.om_recon_merged_status m
    JOIN params p ON p.reconid = m.reconid AND p.env = m.env
    CROSS JOIN bounds b
    WHERE (m.merged_ts >= b.week_start AND m.merged_ts < b.week_end)
       OR (m.modified_ts >= b.week_start AND m.modified_ts < b.week_end)
    GROUP BY m.reconid, m.env, m.triggered_cob_date, m.merge_feed_id
),
pp AS (
    SELECT
        r.id,
        r.reconname,
        r.reconciliation_id,
        r.loadid,
        r.triggerid,
        r.starttime,
        r.endtime,
        r.runstatus,
        r.portalstatus,
        r.rejectstatus,
        r.lastmodified,
        r.message
    FROM fasttrac.reconciliationrun r
    JOIN params p
      ON r.reconname = p.reconname
      OR r.reconciliation_id = p.reconciliation_id
    CROSS JOIN bounds b
    WHERE (r.starttime >= b.week_start AND r.starttime < b.week_end)
       OR (r.endtime >= b.week_start AND r.endtime < b.week_end)
       OR (r.lastmodified >= b.week_start AND r.lastmodified < b.week_end)
),
pp_match AS (
    SELECT
        m.reconid,
        m.env,
        m.triggered_cob_date,
        m.merge_feed_id,
        p.id AS preprocessor_run_id,
        p.loadid,
        p.triggerid,
        p.starttime AS preprocessor_starttime,
        p.endtime AS preprocessor_endtime,
        p.runstatus,
        p.portalstatus,
        p.rejectstatus,
        p.message,
        ROW_NUMBER() OVER (
            PARTITION BY m.reconid, m.env, m.triggered_cob_date, m.merge_feed_id
            ORDER BY p.starttime
        ) AS rn
    FROM ms m
    LEFT JOIN pp p
      ON p.starttime >= NVL(m.merged_ts, m.merged_status_modified_ts)
     AND p.starttime <  NVL(m.merged_ts, m.merged_status_modified_ts) + 1
),
job_status AS (
    SELECT
        d.reconid,
        LISTAGG(
            d.jobid || ':' || rs.status || '/' || rs.spark_state || '/' || rs.spark_finalstate,
            '; '
        ) WITHIN GROUP (ORDER BY rs.created_ts) AS spark_statuses_this_week
    FROM fasttrac.om_recon_datamerge_detail d
    JOIN params p ON p.reconid = d.reconid
    LEFT JOIN fasttrac.om_recon_run_status rs ON rs.jobid = d.jobid
    CROSS JOIN bounds b
    WHERE rs.created_ts >= b.week_start
      AND rs.created_ts < b.week_end
    GROUP BY d.reconid
)
SELECT
    d.reconid,
    m.env,
    d.triggered_cob_date,
    m.merge_feed_id,
    p.preprocessor_run_id,
    p.loadid,
    p.triggerid,
    d.dm_created_ts,
    d.dm_modified_ts,
    m.merged_ts,
    m.merged_status_modified_ts,
    p.preprocessor_starttime,
    p.preprocessor_endtime,
    d.dm_filecount,
    m.merged_file_count,
    m.loaded_count,
    m.ismerged,
    m.issla_breached,
    p.runstatus,
    p.portalstatus,
    p.rejectstatus,
    js.spark_statuses_this_week,
    ROUND(
        (CAST(NVL(d.dm_modified_ts, d.dm_created_ts) AS DATE) - CAST(d.dm_created_ts AS DATE)) * 1440,
        2
    ) AS dm_metadata_duration_min,
    ROUND(
        (CAST(NVL(m.merged_ts, m.merged_status_modified_ts) AS DATE)
            - CAST(NVL(d.dm_modified_ts, d.dm_created_ts) AS DATE)) * 1440,
        2
    ) AS dm_modified_to_merged_status_min,
    ROUND(
        (CAST(p.preprocessor_starttime AS DATE)
            - CAST(NVL(m.merged_ts, m.merged_status_modified_ts) AS DATE)) * 1440,
        2
    ) AS merged_status_to_preprocessor_start_min,
    ROUND(
        (CAST(p.preprocessor_starttime AS DATE) - CAST(d.dm_created_ts AS DATE)) * 1440,
        2
    ) AS total_dm_created_to_preprocessor_start_min
FROM dm d
LEFT JOIN ms m
  ON m.reconid = d.reconid
 AND (
        m.triggered_cob_date = d.triggered_cob_date
     OR (m.triggered_cob_date IS NULL AND d.triggered_cob_date IS NULL)
 )
LEFT JOIN pp_match p
  ON p.reconid = m.reconid
 AND p.env = m.env
 AND p.triggered_cob_date = m.triggered_cob_date
 AND p.merge_feed_id = m.merge_feed_id
 AND p.rn = 1
LEFT JOIN job_status js ON js.reconid = d.reconid
ORDER BY d.triggered_cob_date, d.dm_created_ts, m.merged_ts;


/* ============================================================================
   02. Scheduler config for the recon
   ============================================================================ */

SELECT *
FROM fasttrac.om_recon_schedule
WHERE reconid = <<RECONID>>
  AND env = '<<ENV>>';


/* ============================================================================
   03. Data Merger records this week
   ============================================================================ */

SELECT *
FROM fasttrac.om_recon_datamerge
WHERE reconid = <<RECONID>>
  AND (
        created_ts >= TRUNC(SYSDATE, 'IW') AND created_ts < TRUNC(SYSDATE, 'IW') + 7
     OR modified_ts >= TRUNC(SYSDATE, 'IW') AND modified_ts < TRUNC(SYSDATE, 'IW') + 7
  )
ORDER BY triggered_cob_date, created_ts, modified_ts;


/* ============================================================================
   04. Data Merger detail records
   ============================================================================ */

SELECT *
FROM fasttrac.om_recon_datamerge_detail
WHERE reconid = <<RECONID>>
ORDER BY jobid, filepattern;


/* ============================================================================
   05. Merged Status records this week
   ============================================================================ */

SELECT *
FROM fasttrac.om_recon_merged_status
WHERE reconid = <<RECONID>>
  AND env = '<<ENV>>'
  AND (
        merged_ts >= TRUNC(SYSDATE, 'IW') AND merged_ts < TRUNC(SYSDATE, 'IW') + 7
     OR modified_ts >= TRUNC(SYSDATE, 'IW') AND modified_ts < TRUNC(SYSDATE, 'IW') + 7
  )
ORDER BY triggered_cob_date, merged_ts, modified_ts;


/* ============================================================================
   06. Reconciliation / Preprocessor runs this week
   ============================================================================ */

SELECT *
FROM fasttrac.reconciliationrun
WHERE (reconname = '<<RECONNAME>>' OR reconciliation_id = <<RECONCILIATION_ID>>)
  AND (
        starttime >= TRUNC(SYSDATE, 'IW') AND starttime < TRUNC(SYSDATE, 'IW') + 7
     OR endtime >= TRUNC(SYSDATE, 'IW') AND endtime < TRUNC(SYSDATE, 'IW') + 7
     OR lastmodified >= TRUNC(SYSDATE, 'IW') AND lastmodified < TRUNC(SYSDATE, 'IW') + 7
  )
ORDER BY starttime, endtime;


/* ============================================================================
   07. Spark job statuses this week by Data Merger detail JOBID
   ============================================================================ */

SELECT
    d.reconid,
    d.jobid,
    rs.status,
    rs.spark_state,
    rs.spark_finalstate,
    rs.created_ts,
    rs.created_by
FROM fasttrac.om_recon_datamerge_detail d
LEFT JOIN fasttrac.om_recon_run_status rs ON rs.jobid = d.jobid
WHERE d.reconid = <<RECONID>>
  AND rs.created_ts >= TRUNC(SYSDATE, 'IW')
  AND rs.created_ts < TRUNC(SYSDATE, 'IW') + 7
ORDER BY rs.created_ts, d.jobid;


/* ============================================================================
   08. Failed, retried, pending, or still-running Spark records this week
   ============================================================================ */

SELECT *
FROM fasttrac.om_recon_run_status
WHERE created_ts >= TRUNC(SYSDATE, 'IW')
  AND created_ts < TRUNC(SYSDATE, 'IW') + 7
  AND (
        UPPER(NVL(status, 'UNKNOWN')) IN (
            'FAILED', 'FAILURE', 'ERROR', 'KILLED', 'PENDING',
            'RUNNING', 'RETRY', 'RETRIED'
        )
     OR UPPER(NVL(spark_state, 'UNKNOWN')) IN (
            'FAILED', 'KILLED', 'PENDING', 'RUNNING'
        )
     OR UPPER(NVL(spark_finalstate, 'UNKNOWN')) NOT IN (
            'SUCCEEDED', 'SUCCESS', 'COMPLETED'
        )
  )
ORDER BY created_ts;
