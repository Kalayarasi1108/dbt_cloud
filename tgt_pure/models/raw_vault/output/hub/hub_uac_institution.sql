INSERT INTO DATA_VAULT.CORE.HUB_UAC_INSTITUTION(HUB_UAC_INSTITUTION_KEY, CODE, YEAR, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(
                   IFNULL(A.CODE, '') || ',' ||
                   IFNULL(A.YEAR, '') || ',' ||
                   IFNULL(A.SOURCE, '')
           )                                     HUB_UAC_INSTITUTION_KEY,
       A.CODE                                    CODE,
       A.YEAR                                    YEAR,
       A.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.UAC.VW_ALL_INST A
WHERE NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.HUB_UAC_INSTITUTION S
        WHERE S.HUB_UAC_INSTITUTION_KEY = MD5(
                    IFNULL(A.CODE, '') || ',' ||
                    IFNULL(A.YEAR, '') || ',' ||
                    IFNULL(A.SOURCE, '')
            )
    );