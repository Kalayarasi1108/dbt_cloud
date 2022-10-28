INSERT INTO DATA_VAULT.CORE.HUB_COURSE_APPLICATION(HUB_COURSE_APPLICATION_KEY, STU_ID, SPK_NO,
                                                                  SPK_VER_NO, APPN_NO, SOURCE, ETL_JOB_ID, LOAD_DTS)
SELECT HUB_COURSE_APPLICATION_KEY,
       STU_ID,
       SPK_NO,
       SPK_VER_NO,
       APPN_NO,
       SOURCE,
       ETL_JOB_ID,
       LOAD_DTS
FROM (
         SELECT HUB_COURSE_APPLICATION_KEY,
                STU_ID,
                SPK_NO,
                SPK_VER_NO,
                APPN_NO,
                SOURCE,
                ETL_JOB_ID,
                LOAD_DTS
         FROM (
                  SELECT MD5(IFNULL(s.STU_ID, '') || ',' ||
                             IFNULL(SPK_NO, 0) || ',' ||
                             IFNULL(SPK_VER_NO, 0) || ',' ||
                             (CONCAT(s.application_id, '_', s.application_line_id)))
                                                                                 HUB_COURSE_APPLICATION_KEY,
                         s.STU_ID,
                         SPK_NO,
                         SPK_VER_NO,
                         CONCAT(s.application_id, '_', s.application_line_id) as APPN_NO,
                         'AMIS'                                                  SOURCE,
                         CURRENT_TIMESTAMP::TIMESTAMP_NTZ                        LOAD_DTS,
                         'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ               ETL_JOB_ID
                  FROM ODS.AMIS.S1APP_APPLICATION as a
                           INNER JOIN ODS.AMIS.S1APP_APPLICATION_LINE as l on a.application_id = l.application_id
                           INNER JOIN ODS.AMIS.S1APP_STUDY as s on l.application_id = s.application_id
                      and s.application_line_id = l.application_line_id
                           LEFT JOIN ODS.AMIS.S1APP_OFFER as o on o.application_id = l.application_id
                      and l.application_line_id = o.application_line_id
              ) as A
     ) as D
WHERE NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.HUB_COURSE_APPLICATION as h
        where h.hub_course_application_key = d.hub_course_application_key
    );