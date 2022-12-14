INSERT INTO DATA_VAULT.CORE.SAT_SES_SURVEY (SAT_SES_SURVEY_SK, HUB_SES_SURVEY_KEY, SOURCE,
                                            LOAD_DTS,
                                            ETL_JOB_ID, HASH_MD5, IS_DELETED)
WITH SAT_SES_LATEST AS (
    SELECT HUB_SES_SURVEY_KEY,
           HASH_MD5,
           LOAD_DTS,
           IS_DELETED,
           LEAD(LOAD_DTS) OVER (PARTITION BY HUB_SES_SURVEY_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
    FROM DATA_VAULT.CORE.SAT_SES_SURVEY
)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_SES_SURVEY_SK,
       S.HUB_SES_SURVEY_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM SAT_SES_LATEST S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.QILT.SES_MQ_PIVOT SES_MQ
        WHERE S.HUB_SES_SURVEY_KEY = MD5(IFNULL(SES_MQ.SESID, ''))
    );