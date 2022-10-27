INSERT INTO DATA_VAULT.CORE.SAT_UNIT_ENROLMENT (SAT_UNIT_ENROLMENT_SK, HUB_UNIT_ENROLMENT_KEY, SOURCE, LOAD_DTS,
                                                ETL_JOB_ID, HASH_MD5, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_UNIT_ENROLMENT_SK,
       S.HUB_UNIT_ENROLMENT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB_UNIT_ENROLMENT_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_UNIT_ENROLMENT_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_UNIT_ENROLMENT
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
                 JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
                      ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                          AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                          AND UN_SPK_DET.SPK_CAT_CD = 'UN'
        WHERE UN_SSP.SSP_NO != UN_SSP.PARENT_SSP_NO
          AND S.HUB_UNIT_ENROLMENT_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                             IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                                             IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                                             IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                                             IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                                             IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.PARENT_SSP_NO, 0)
            )
    )
;