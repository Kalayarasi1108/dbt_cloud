INSERT INTO DATA_VAULT.CORE.SAT_STRUCTURE_AMIS (SAT_STRUCTURE_AMIS_SK,
                                            HUB_STRUCTURE_KEY,
                                            SOURCE,
                                            LOAD_DTS,
                                            ETL_JOB_ID,
                                            HASH_MD5,
                                            IS_DELETED)

SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_STRUCTURE_AMIS_SK,
       S.HUB_STRUCTURE_KEY                           HUB_STRUCTURE_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       DELETED
FROM (
         SELECT SAT.HUB_STRUCTURE_KEY,
                SAT.SPK_CD,
                SAT.SPK_VER_NO,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_STRUCTURE_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_STRUCTURE_AMIS SAT
                  JOIN DATA_VAULT.CORE.HUB_STRUCTURE HUB
                       ON SAT.HUB_STRUCTURE_KEY = HUB.HUB_STRUCTURE_KEY
                           AND HUB.SOURCE = 'AMIS'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM (
                 SELECT STRUC_SPK_DET.SPK_CD,
                        STRUC_SPK_DET.SPK_VER_NO
                 FROM ODS.AMIS.S1SPK_DET STRUC_SPK_DET
                 WHERE STRUC_SPK_DET.SPK_CAT_CD IN (
                                                 'SS',
                                                 'MJ',
                                                 'SP',
                                                 'MN',
                                                 'ST'
                     )
             ) MAJOR
        WHERE S.SPK_CD = MAJOR.SPK_CD
          AND S.SPK_VER_NO = MAJOR.SPK_VER_NO
    )
;
