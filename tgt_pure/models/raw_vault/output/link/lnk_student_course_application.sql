INSERT INTO DATA_VAULT.CORE.LNK_STUDENT_COURSE_APPLICATION (LNK_STUDENT_COURSE_APPLICATION_KEY,
                                                                           HUB_STUDENT_KEY,
                                                                           HUB_COURSE_APPLICATION_KEY,
                                                                           STU_ID,
                                                                           SPK_NO,
                                                                           SPK_VER_NO,
                                                                           APPN_NO,
                                                                           SOURCE,
                                                                           LOAD_DTS,
                                                                           ETL_JOB_ID)

WITH X AS (
    SELECT MD5(
                       IFNULL(STU_ID, '') || ',' ||
                       IFNULL(SPK_NO, 0) || ',' ||
                       IFNULL(SPK_VER_NO, 0) || ',' ||
                       IFNULL(CONCAT(APPLICATION_ID, '_', APPLICATION_LINE_ID), '')
               )                                                             LNK_STUDENT_COURSE_APPLICATION_KEY,
           MD5(APP.STU_ID)                                                   HUB_STUDENT_KEY,
           MD5(
                       IFNULL(STU_ID, '') || ',' ||
                       IFNULL(SPK_NO, 0) || ',' ||
                       IFNULL(SPK_VER_NO, 0) || ',' ||
                       IFNULL(CONCAT(APPLICATION_ID, '_', APPLICATION_LINE_ID), '')
               )                                                             HUB_COURSE_APPLICATION_KEY,
           STU_ID,
           SPK_NO,
           SPK_VER_NO,
           CONCAT(APPLICATION_ID, '_', APPLICATION_LINE_ID)::varchar(200) as APPN_NO,
           'AMIS'                                                            SOURCE,
           CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                  LOAD_DTS,
           'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ                         ETL_JOB_ID
    FROM ODS.AMIS.S1APP_STUDY APP
)
SELECT LNK_STUDENT_COURSE_APPLICATION_KEY,
       HUB_STUDENT_KEY,
       HUB_COURSE_APPLICATION_KEY,
       STU_ID,
       SPK_NO,
       SPK_VER_NO,
       APPN_NO,
       SOURCE,
       LOAD_DTS,
       ETL_JOB_ID
FROM X
WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_STUDENT_COURSE_APPLICATION L
        WHERE L.LNK_STUDENT_COURSE_APPLICATION_KEY = X.LNK_STUDENT_COURSE_APPLICATION_KEY
    );
