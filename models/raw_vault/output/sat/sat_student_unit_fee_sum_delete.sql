INSERT INTO DATA_VAULT.CORE.SAT_STUDENT_UNIT_FEE_SUM (SAT_STUDENT_UNIT_FEE_SUM_SK, HUB_STUDENT_UNIT_FEE_KEY, SOURCE, LOAD_DTS,
                                                  ETL_JOB_ID, HASH_MD5, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_STUDENT_UNIT_FEE_SUM_SK,
       S.HUB_STUDENT_UNIT_FEE_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB_STUDENT_UNIT_FEE_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_STUDENT_UNIT_FEE_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_STUDENT_UNIT_FEE_SUM
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
                 JOIN ODS.AMIS.S1STU_FEES STU_FEES
                      ON STU_FEES.SSP_NO = UN_SSP.SSP_NO
        WHERE UN_SSP.SSP_NO != UN_SSP.PARENT_SSP_NO
          AND S.HUB_STUDENT_UNIT_FEE_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                               IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                                               IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                               IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                                               IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                                               IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                                               IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                               IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                                               IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
                                               IFNULL(STU_FEES.EP_YEAR, 0) || ',' ||
                                               IFNULL(STU_FEES.EP_NO, 0) || ',' ||
                                               IFNULL(STU_FEES.FEE_SEQ_NO, 0)
            )
    )
;