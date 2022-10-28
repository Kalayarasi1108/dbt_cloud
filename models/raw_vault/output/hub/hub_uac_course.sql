INSERT INTO DATA_VAULT.CORE.HUB_UAC_COURSE (HUB_UAC_COURSE_KEY, YEAR, SOURCE, LOAD_DTS, ETL_JOB_ID, COURSE)
SELECT MD5(IFNULL(A.COURSE, 0) || ',' ||
           A.YEAR || ',' ||
           A.SOURCE)                             HUB_UAC_COURSE_KEY,
       A.YEAR                                    YEAR,
       A.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       A.COURSE                                  COURSE
FROM (
         SELECT COURSE, YEAR, SOURCE
         FROM ODS.UAC.VW_ALL_COURSE
     ) A
WHERE NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.HUB_UAC_COURSE S
        WHERE S.HUB_UAC_COURSE_KEY = MD5(IFNULL(A.COURSE, 0) || ',' ||
                                         A.YEAR || ',' ||
                                         A.SOURCE)
    );