INSERT INTO DATA_VAULT.CORE.SAT_UNIT_OFFERING_AMIS (SAT_UNIT_OFFERING_AMIS_SK,
                                                    HUB_UNIT_OFFERING_KEY,
                                                    SOURCE,
                                                    LOAD_DTS,
                                                    ETL_JOB_ID,
                                                    HASH_MD5,
                                                    IS_DELETED)

SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_UNIT_OFFERING_AMIS_SK,
       S.HUB_UNIT_OFFERING_KEY                   HUB_UNIT_OFFERING_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       DELETED
FROM (
         SELECT SAT.HUB_UNIT_OFFERING_KEY,
                SAT.SPK_CD,
                SAT.SPK_VER_NO,
                SAT.AVAIL_KEY_NO,
                SAT.AVAIL_YR,
                SAT.SPRD_CD,
                SAT.LOCATION_CD,
                LEAD(SAT.LOAD_DTS)
                     OVER (PARTITION BY SAT.HUB_UNIT_OFFERING_KEY ORDER BY SAT.LOAD_DTS ASC) EFFECTIVE_END_DTS,
                IS_DELETED
         FROM DATA_VAULT.CORE.SAT_UNIT_OFFERING_AMIS SAT
                  JOIN DATA_VAULT.CORE.HUB_UNIT_OFFERING HUB
                       ON SAT.HUB_UNIT_OFFERING_KEY = HUB.HUB_UNIT_OFFERING_KEY
                           AND HUB.SOURCE = 'AMIS'
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM (
                 SELECT UN_SPK_DET.SPK_CD,
                        UN_SPK_DET.SPK_VER_NO,
                        UN_AVAIL_DET.AVAIL_KEY_NO,
                        UN_AVAIL_DET.AVAIL_YR,
                        UN_AVAIL_DET.SPRD_CD,
                        UN_AVAIL_DET.LOCATION_CD
                 FROM ODS.AMIS.S1SPK_DET UN_SPK_DET
                          JOIN ODS.AMIS.S1SPK_AVAIL_DET UN_AVAIL_DET
                               ON UN_AVAIL_DET.SPK_NO = UN_SPK_DET.SPK_NO AND
                                  UN_AVAIL_DET.SPK_VER_NO = UN_SPK_DET.SPK_VER_NO
             ) UOA
        WHERE S.SPK_CD = UOA.SPK_CD
          AND S.SPK_VER_NO = UOA.SPK_VER_NO
          AND S.AVAIL_KEY_NO = UOA.AVAIL_KEY_NO
          AND S.AVAIL_YR = UOA.AVAIL_YR
          AND S.SPRD_CD = UOA.SPRD_CD
          AND S.LOCATION_CD = UOA.LOCATION_CD
    )
;
