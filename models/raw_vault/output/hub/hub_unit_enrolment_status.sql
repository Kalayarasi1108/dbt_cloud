INSERT INTO DATA_VAULT.CORE.HUB_UNIT_ENROLMENT_STATUS (HUB_UNIT_ENROLMENT_STATUS_KEY,
                                                       STU_ID,
                                                       SSP_NO,
                                                       SSP_STTS_NO,
                                                       SOURCE,
                                                       LOAD_DTS,
                                                       ETL_JOB_ID)
SELECT MD5(IFNULL(UN_SSP_STTS.STU_ID, '') || ',' ||
           IFNULL(UN_SSP_STTS.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP_STTS.SSP_STTS_NO, 0)
           )                                     HUB_UNIT_ENROLMENT_STATUS_KEY,
       UN_SSP_STTS.STU_ID,
       UN_SSP_STTS.SSP_NO,
       UN_SSP_STTS.SSP_STTS_NO,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SSP_STTS_HIST UN_SSP_STTS
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP_STTS.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP_STTS.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.HUB_UNIT_ENROLMENT_STATUS H
        WHERE H.HUB_UNIT_ENROLMENT_STATUS_KEY =
              MD5(IFNULL(UN_SSP_STTS.STU_ID, '') || ',' ||
                  IFNULL(UN_SSP_STTS.SSP_NO, 0) || ',' ||
                  IFNULL(UN_SSP_STTS.SSP_STTS_NO, 0)
                  )
    )
;