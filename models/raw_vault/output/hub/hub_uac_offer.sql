INSERT INTO DATA_VAULT.CORE.HUB_UAC_OFFER (HUB_UAC_OFFER_KEY, YEAR, SOURCE, LOAD_DTS, ETL_JOB_ID, ROUNDNUM, REFNUM, COURSE )
SELECT MD5(IFNULL(A.ROUNDNUM, 0) || ',' ||
                    IFNULL(A.REFNUM, 0) || ',' ||
                    IFNULL(A.COURSE, 0) || ',' ||
                    A.YEAR || ',' ||
                    A.SOURCE)                             HUB_UAC_OFFER_KEY,
       A.YEAR                                    YEAR,
       A.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       A.ROUNDNUM ROUNDNUM,
       A.REFNUM                                  REFNUM,
       A.COURSE COURSE
FROM (
         SELECT ROUNDNUM, REFNUM, COURSE, YEAR, SOURCE
         FROM ODS.UAC.VW_ALL_OFFER
     ) A
WHERE NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.HUB_UAC_OFFER S
        WHERE S.HUB_UAC_OFFER_KEY = MD5(IFNULL(A.ROUNDNUM, 0) || ',' ||
                                        IFNULL(A.REFNUM, 0) || ',' ||
                                        IFNULL(A.COURSE, 0) || ',' ||
                                        A.YEAR || ',' ||
                                        A.SOURCE)
    );