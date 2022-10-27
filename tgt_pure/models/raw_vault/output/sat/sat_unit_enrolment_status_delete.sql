INSERT INTO DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_STATUS (SAT_UNIT_ENROLMENT_STATUS_SK, HUB_UNIT_ENROLMENT_STATUS_KEY, SOURCE,
                                                       LOAD_DTS,
                                                       ETL_JOB_ID, HASH_MD5, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_UNIT_ENROLMENT_STATUS_SK,
       S.HUB_UNIT_ENROLMENT_STATUS_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5('')                                   HASH_MD5,
       'Y'                                       IS_DELETED
FROM (
         SELECT HUB_UNIT_ENROLMENT_STATUS_KEY,
                HASH_MD5,
                LOAD_DTS,
                IS_DELETED,
                LEAD(LOAD_DTS) OVER (PARTITION BY HUB_UNIT_ENROLMENT_STATUS_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
         FROM DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_STATUS
     ) S
WHERE S.EFFECTIVE_END_DTS IS NULL
  AND S.IS_DELETED = 'N'
  AND NOT EXISTS(
        SELECT NULL
        FROM ODS.AMIS.S1SSP_STTS_HIST UN_SSP_STTS
                 JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
                      ON UN_SPK_DET.SPK_NO = UN_SSP_STTS.SPK_NO
                          AND UN_SPK_DET.SPK_VER_NO = UN_SSP_STTS.SPK_VER_NO
                          AND UN_SPK_DET.SPK_CAT_CD = 'UN'
        WHERE S.HUB_UNIT_ENROLMENT_STATUS_KEY = MD5(IFNULL(UN_SSP_STTS.STU_ID, '') || ',' ||
                                                    IFNULL(UN_SSP_STTS.SSP_NO, 0) || ',' ||
                                                    IFNULL(UN_SSP_STTS.SSP_STTS_NO, 0)
            )
    )
;