INSERT INTO DATA_VAULT.CORE.HUB_UAC_PREFERENCE (HUB_UAC_PREFERENCE_KEY, YEAR, SOURCE, LOAD_DTS, ETL_JOB_ID, PREFNUM,
                                                SETNUM, REFNUM, COURSE)
SELECT MD5(IFNULL(A.PREFNUM, 0) || ',' ||
           IFNULL(A.SETNUM, 0) || ',' ||
           IFNULL(A.REFNUM, 0) || ',' ||
           IFNULL(A.COURSE, 0) || ',' ||
           A.YEAR || ',' ||
           A.SOURCE)                             HUB_UAC_PREFERENCE_KEY,
       A.YEAR                                    YEAR,
       A.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       A.PREFNUM                                 ROUNDNUM,
       A.SETNUM                                  SETNUM,
       A.REFNUM                                  REFNUM,
       A.COURSE                                  COURSE
FROM (
         SELECT PREFNUM, SETNUM, REFNUM, COURSE, YEAR, SOURCE
         FROM ODS.UAC.VW_ALL_PREF
     ) A
WHERE NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.HUB_UAC_PREFERENCE S
        WHERE S.HUB_UAC_PREFERENCE_KEY = MD5(IFNULL(A.PREFNUM, 0) || ',' ||
                                             IFNULL(A.SETNUM, 0) || ',' ||
                                             IFNULL(A.REFNUM, 0) || ',' ||
                                             IFNULL(A.COURSE, 0) || ',' ||
                                             A.YEAR || ',' ||
                                             A.SOURCE)
    );