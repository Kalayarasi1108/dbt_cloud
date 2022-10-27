INSERT INTO DATA_VAULT.CORE.HUB_UAC_APPLICANT (HUB_UAC_APPLICANT_KEY, YEAR, SOURCE, LOAD_DTS, ETL_JOB_ID, REFNUM)
SELECT MD5(IFNULL(A.REFNUM, 0) || ',' ||
           A.YEAR || ',' ||
           A.SOURCE)                             HUB_UAC_APPLICANT_KEY,
       A.YEAR                                    YEAR,
       A.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       A.REFNUM                                  REFNUM
FROM (
         SELECT REFNUM, YEAR, SOURCE
         FROM ODS.UAC.VW_ALL_APPLIC
     ) A
WHERE NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.HUB_UAC_APPLICANT S
        WHERE S.HUB_UAC_APPLICANT_KEY = MD5(IFNULL(A.REFNUM, 0) || ',' ||
                                            A.YEAR || ',' ||
                                            A.SOURCE)
    );