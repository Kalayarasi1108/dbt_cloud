-- DELETE
INSERT INTO DATA_VAULT.CORE.SAT_ORG_UNIT_AMIS (SAT_ORG_UNIT_AMIS_SK, HUB_ORG_UNIT_KEY, SOURCE, LOAD_DTS, ETL_JOB_ID,
                                               HASH_MD5, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_ORG_UNIT_AMIS_SK,
       S.HUB_ORG_UNIT_KEY                        HUB_ORG_UNIT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB.ORG_UNIT_CD,
                SAT.HUB_ORG_UNIT_KEY,
                SAT.LOAD_DTS,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_ORG_UNIT_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_ORG_UNIT_AMIS SAT
                  JOIN DATA_VAULT.CORE.HUB_ORG_UNIT HUB
                       ON HUB.HUB_ORG_UNIT_KEY = SAT.HUB_ORG_UNIT_KEY AND HUB.SOURCE = 'AMIS'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1ORG_UNIT ORG
        WHERE ORG.ORG_UNIT_CD = S.ORG_UNIT_CD
    )
;