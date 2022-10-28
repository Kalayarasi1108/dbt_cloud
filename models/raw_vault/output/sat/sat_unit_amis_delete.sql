INSERT INTO DATA_VAULT.CORE.SAT_UNIT_AMIS (SAT_UNIT_AMIS_SK,
                                           HUB_UNIT_KEY,
                                           SOURCE,
                                           LOAD_DTS,
                                           ETL_JOB_ID,
                                           HASH_MD5,
                                           IS_DELETED)

SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_UNIT_AMIS_SK,
       S.HUB_UNIT_KEY                            HUB_UNIT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       DELETED
FROM (
         SELECT SAT.HUB_UNIT_KEY,
                SAT.SPK_CD,
                SAT.SPK_VER_NO,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_UNIT_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_UNIT_AMIS SAT
                  JOIN DATA_VAULT.CORE.HUB_UNIT HUB
                       ON SAT.HUB_UNIT_KEY = HUB.HUB_UNIT_KEY
                           AND HUB.SOURCE = 'AMIS'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM (
                 SELECT UN_SPK_DET.SPK_CD,
                        UN_SPK_DET.SPK_VER_NO
                 FROM ODS.AMIS.S1SPK_DET UN_SPK_DET
                 WHERE UN_SPK_DET.SPK_CAT_CD = 'UN'
             ) UNIT
        WHERE S.SPK_CD = UNIT.SPK_CD
          AND S.SPK_VER_NO = UNIT.SPK_VER_NO
    )
;
