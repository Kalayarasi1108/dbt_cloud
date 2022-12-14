-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_ORG_UNIT_AMIS_SUM(SAT_ORG_UNIT_AMIS_SUM_SK, HUB_ORG_UNIT_KEY, SOURCE, LOAD_DTS,
                                                  ETL_JOB_ID, HASH_MD5, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_APPLICATION_SUM_SK,
       S.HUB_ORG_UNIT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB_ORG_UNIT_KEY,
                ORG_UNIT_CD,
                EFFECTIVE_DATE,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_ORG_UNIT_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_ORG_UNIT_AMIS_SUM
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1ORG_UNIT ORG
        WHERE ORG.ORG_UNIT_CD = S.ORG_UNIT_CD
          and org.EFFCT_DT = S.EFFECTIVE_DATE)
;