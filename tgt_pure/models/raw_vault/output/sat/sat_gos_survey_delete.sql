INSERT INTO DATA_VAULT.CORE.SAT_GOS_SURVEY (SAT_GOS_SURVEY_SK, HUB_GOS_SURVEY_KEY, SOURCE,
                                                      LOAD_DTS,
                                                      ETL_JOB_ID, HASH_MD5, IS_DELETED)
WITH SAT_GOS_LATEST AS (
    SELECT HUB_GOS_SURVEY_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_GOS_SURVEY_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_GOS_SURVEY
)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_GOS_SURVEY_SK,
       S.HUB_GOS_SURVEY_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM  SAT_GOS_LATEST S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.QILT.GOS_MQ_PIVOT GOS_MQ
      WHERE S.HUB_GOS_SURVEY_KEY = MD5(IFNULL(GOS_MQ.GOSID, ''))
    );