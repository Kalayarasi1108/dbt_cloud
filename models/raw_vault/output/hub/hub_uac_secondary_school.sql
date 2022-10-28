INSERT INTO DATA_VAULT.CORE.HUB_UAC_SECONDARY_SCHOOL(HUB_UAC_SECONDARY_SCHOOL_KEY, CODE, YEAR, SOURCE, STATE,
                                                     LOAD_DTS,
                                                     ETL_JOB_ID)
SELECT MD5(
                   IFNULL(A.CODE, '') || ',' ||
                   IFNULL(A.YEAR, '') || ',' ||
                   IFNULL(A.SOURCE, '') || ',' ||
                   IFNULL(A.STATE, '')
           )                                     HUB_UAC_SECONDARY_SCHOOL_KEY,
       A.CODE                                    CODE,
       A.YEAR                                    YEAR,
       A.SOURCE                                  SOURCE,
       A.STATE                                   STATE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.UAC.VW_ALL_SCHOOL A
WHERE NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.HUB_UAC_SECONDARY_SCHOOL S
        WHERE S.HUB_UAC_SECONDARY_SCHOOL_KEY = MD5(
                    IFNULL(A.CODE, '') || ',' ||
                    IFNULL(A.YEAR, '') || ',' ||
                    IFNULL(A.SOURCE, '') || ',' ||
                    IFNULL(A.STATE, '')
            )
    );


