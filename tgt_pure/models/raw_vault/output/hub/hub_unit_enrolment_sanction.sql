INSERT INTO DATA_VAULT.CORE.HUB_UNIT_ENROLMENT_SANCTION(HUB_UNIT_ENROLMENT_SANCTION_KEY, UN_SSP_NO, SEQ_NO, STU_ID,
                                                        SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(
                   IFNULL(SCT_DTL.SSP_NO, 0) || ',' ||
                   IFNULL(SCT_DTL.SEQ_NO, 0) || ',' ||
                   IFNULL(SCT_DTL.STU_ID, '')
           )                                       HUB_UNIT_ENROLMENT_SANCTION_KEY,
       SCT_DTL.SSP_NO,
       SCT_DTL.SEQ_NO,
       SCT_DTL.STU_ID,
       'AMIS'                                      SOURCE,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SSP_SCT_DTL SCT_DTL
         JOIN ODS.AMIS.S1SSP_STU_SPK STU_SPK ON STU_SPK.SSP_NO = SCT_DTL.SSP_NO
         JOIN ODS.AMIS.S1SPK_DET SPK_DET ON STU_SPK.SPK_NO = SPK_DET.SPK_NO and STU_SPK.SPK_VER_NO = SPK_DET.SPK_VER_NO
    AND SPK_DET.SPK_CAT_CD = 'UN'
WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.HUB_UNIT_ENROLMENT_SANCTION H
        WHERE H.HUB_UNIT_ENROLMENT_SANCTION_KEY = MD5(
                    IFNULL(SCT_DTL.SSP_NO, 0) || ',' ||
                    IFNULL(SCT_DTL.SEQ_NO, 0) || ',' ||
                    IFNULL(SCT_DTL.STU_ID, '')
            )
    );