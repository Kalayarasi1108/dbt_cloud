INSERT INTO DATA_VAULT.CORE.HUB_UAC_QUALIFICATION (HUB_UAC_QUALIFICATION_KEY, YEAR, SOURCE, LOAD_DTS, ETL_JOB_ID,
                                                   REFNUM, QUALNUM)
SELECT MD5(IFNULL(A.REFNUM, 0) || ',' ||
           IFNULL(A.QUALNUM, 0) || ',' ||
           A.YEAR || ',' ||
           A.SOURCE)                             HUB_UAC_QUALIFICATION_KEY,
       A.YEAR                                    YEAR,
       A.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       A.REFNUM                                  REFNUM,
       A.QUALNUM                                 QUALNUM
FROM (
         SELECT REFNUM, QUALNUM, YEAR, SOURCE
         FROM ODS.UAC.VW_ALL_QUAL
     ) A
WHERE NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.HUB_UAC_QUALIFICATION S
        WHERE S.HUB_UAC_QUALIFICATION_KEY = MD5(IFNULL(A.REFNUM, 0) || ',' ||
                                                IFNULL(A.QUALNUM, 0) || ',' ||
                                                A.YEAR || ',' ||
                                                A.SOURCE)
    );