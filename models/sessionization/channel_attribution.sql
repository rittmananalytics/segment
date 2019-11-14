{{
  config( materialized='table')
}}

WITH session_attribution AS
(
   SELECT
      *,
      CASE
         WHEN
            session_id = last_value(session_id) OVER (PARTITION BY worker_id ORDER BY
            session_start_tstamp ROWS BETWEEN unbounded preceding AND unbounded following)
         THEN 1 ELSE 0 END AS last_click_attrib_pct,
      CASE WHEN
            session_id = FIRST_VALUE(session_id) OVER (PARTITION BY worker_id
   ORDER BY
      session_start_tstamp ROWS BETWEEN unbounded preceding AND unbounded following)
   THEN
      1
   ELSE
      0
      END
      AS first_click_attrib_pct,
      CASE
         WHEN
            session_id = last_value(session_id ignore nulls) OVER (PARTITION BY worker_id
   ORDER BY
      session_start_tstamp ROWS BETWEEN unbounded preceding AND unbounded following)
   THEN
      1
   ELSE
      0
      END
      AS last_non_direct_click_attrib_pct, 1 / COUNT(session_id) OVER (PARTITION BY worker_id) AS even_click_attrib_pct
   FROM
      (
         SELECT
            w.nurse_full_name,
            s.session_start_tstamp,
            s.session_end_tstamp,
            w.created_at,
            w.id AS worker_id,
            s.session_id,
            ROW_NUMBER() OVER (PARTITION BY w.id
         ORDER BY
            s.session_start_tstamp) AS session_seq,
            CASE
               WHEN
                  w.created_at BETWEEN s.session_start_tstamp AND s.session_end_tstamp
               THEN
                  TRUE
               ELSE
                  FALSE
            END
            AS conversion_session,
            CASE
               WHEN
                  w.created_at < s.session_start_tstamp
               THEN
                  TRUE
               ELSE
                  FALSE
            END
            AS prospect_session, s.utm_source, s.utm_content, s.utm_medium, s.utm_campaign, s.first_page_url_path AS entrance_url_path, s.last_page_url_path AS exit_url_path, referrer, duration_in_s
         FROM
            {{ ref('segment_web_sessions__stitched') }} s
            JOIN
               {{ ref('shift_workers') }} w
               ON CAST(w.id AS string) = s.blended_user_id
         WHERE
            w.created_at >= s.session_start_tstamp
         ORDER BY
            w.id, s.session_start_tstamp
      )
)

# replace CTE below with SELECT relevant for customer

, value_attributed AS
(
   SELECT
      worker_id,
      SUM(worker_value) AS total_worker_value
   FROM
      {{ref('timesheets')}}
   WHERE
      approved
   GROUP BY
      1
)

####

SELECT
   session_attribution.*,
   total_worker_value * last_click_attrib_pct AS last_click_attrib_hours_worked,
   total_worker_value * first_click_attrib_pct AS first_click_attrib_hours_worked,
   total_worker_value * even_click_attrib_pct AS even_click_attrib_hours_worked,
   total_worker_value * last_non_direct_click_attrib_pct AS last_non_direct_click_attrib_hours_worked
FROM
   value_attributed
   JOIN
      hours_worked
      ON session_attribution.worker_id = value_attributed.worker_id
